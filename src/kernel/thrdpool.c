/*
  Copyright (c) 2019 Sogou, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

  Author: Xie Han (xiehan@sogou-inc.com)
*/

#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include "msgqueue.h"
#include "thrdpool.h"

struct __thrdpool
{
	msgqueue_t *msgqueue;
	size_t nthreads;
	size_t stacksize;
	pthread_t tid;
	pthread_mutex_t mutex;
	pthread_key_t key;
	pthread_cond_t *terminate;
};

struct __thrdpool_task_entry//先是next指针，后是数据本体,加entry相当于把他从task封装为一个消息队列的msg，具有链表的功能
{
	void *link;
	struct thrdpool_task task;
};

static pthread_t __zero_tid;//？？？？？

static void *__thrdpool_routine(void *arg)//参数为线程池，此方法会从线程池中的消息队列中，取出msg执行，这里的是i__thrdpool_create_threads，创建线程时，线程执行的方法
{
	thrdpool_t *pool = (thrdpool_t *)arg;
	struct __thrdpool_task_entry *entry;
	void (*task_routine)(void *);
	void *task_context;
	pthread_t tid;

	pthread_setspecific(pool->key, pool);//将key和后面的指针相关联，这个key value变量是线程私有的
	while (!pool->terminate)//会从线程池中的msgqueue不断取msg并执行
	{
		entry = (struct __thrdpool_task_entry *)msgqueue_get(pool->msgqueue);//相当于线程池中的每个线程都会竞争从msgqueue中获取entry任务并执行，在开始的时候，msgqueue里没有entry的时候，即没有执行thrdpool_schedule的时候，直接break然后，终止线程？不对吧
		if (!entry)//msg全部执行完
			break;

		task_routine = entry->task.routine;//把msg中的任务中的具体执行函数指针，取出
		task_context = entry->task.context;//任务对应的上下文取出
		free(entry);//取出后可以free了
		task_routine(task_context);//执行？

		if (pool->nthreads == 0)
		{
			/* Thread pool was destroyed by the task. */
			free(pool);
			return NULL;
		}
	}
	//所有线程将msgqueue中的任务执行完了，开始逐个销毁线程
	/* One thread joins another. Don't need to keep all thread IDs. */
	pthread_mutex_lock(&pool->mutex);
	tid = pool->tid;//上一个线程的tid
	pool->tid = pthread_self();//更换为此时线程的tid
	if (--pool->nthreads == 0)//如果这是最后一个线程，则通知pthread_cond_wait(&term, &pool->mutex);，让它对最后一个线程进行join，最终销毁
		pthread_cond_signal(pool->terminate);

	pthread_mutex_unlock(&pool->mutex);
	if (memcmp(&tid, &__zero_tid, sizeof (pthread_t)) != 0)
		pthread_join(tid, NULL);//上一个线程结束之后(join，结束之后会自动销毁线程)，现在这个线程才能结束

	return NULL;
}

static void __thrdpool_terminate(int in_pool, thrdpool_t *pool)//主动终止，比如msg中还有任务，但是某个线程调用此方法，会等待所有线程将自己的entry执行完，然后销毁线程池，即使msgqueue中还有任务
{
	pthread_cond_t term = PTHREAD_COND_INITIALIZER;//？条件变量初始化？

	pthread_mutex_lock(&pool->mutex);
	msgqueue_set_nonblock(pool->msgqueue);
	pool->terminate = &term;

	if (in_pool)//终止pool的线程在pool内
	{
		/* Thread pool destroyed in a pool thread is legal. */
		pthread_detach(pthread_self());//独立此线程和主线程，此线程结束后，线程会自动销毁
		pool->nthreads--;
	}

	while (pool->nthreads > 0)
		pthread_cond_wait(&term, &pool->mutex);//等待n-1个线程结束，销毁

	pthread_mutex_unlock(&pool->mutex);
	if (memcmp(&pool->tid, &__zero_tid, sizeof (pthread_t)) != 0)
		pthread_join(pool->tid, NULL);//最后一个线程的join销毁
}

static int __thrdpool_create_threads(size_t nthreads, thrdpool_t *pool)
{
	pthread_attr_t attr;
	pthread_t tid;
	int ret;

	ret = pthread_attr_init(&attr);
	if (ret == 0)
	{
		if (pool->stacksize)
			pthread_attr_setstacksize(&attr, pool->stacksize);//线程私有的栈空间

		while (pool->nthreads < nthreads)
		{
			ret = pthread_create(&tid, &attr, __thrdpool_routine, pool);//&tid，指向的位置为此线程的tid（pthread_create分配），然后此线程执行后面的方法，下次循环会创建新的线程和tid，然后去执行对应的方法
			if (ret == 0)
				pool->nthreads++;
			else
				break;
		}

		pthread_attr_destroy(&attr);
		if (pool->nthreads == nthreads)
			return 0;

		__thrdpool_terminate(0, pool);//创建失败。销毁线程池，主线程来销毁，因此是0
	}

	errno = ret;
	return -1;
}

thrdpool_t *thrdpool_create(size_t nthreads, size_t stacksize)//创建threadpool，初始化参数
{
	thrdpool_t *pool;
	int ret;

	pool = (thrdpool_t *)malloc(sizeof (thrdpool_t));
	if (!pool)
		return NULL;

	pool->msgqueue = msgqueue_create((size_t)-1, 0);//-1转成无符号的long型，设置msg 中maxlen的大小，创建消息队列，消息队列在哪里put任务呢？__thrdpool_schedule里
	if (pool->msgqueue)
	{
		ret = pthread_mutex_init(&pool->mutex, NULL);
		if (ret == 0)
		{
			ret = pthread_key_create(&pool->key, NULL);//？？？？啥作用，创建一个所有线程可以访问的全局变量？每个线程都可以为这个建赋值，一键多值
			if (ret == 0)
			{
				pool->stacksize = stacksize;
				pool->nthreads = 0;
				memset(&pool->tid, 0, sizeof (pthread_t));
				pool->terminate = NULL;
				if (__thrdpool_create_threads(nthreads, pool) >= 0)//创建线程池中的线程
					return pool;

				pthread_key_delete(pool->key);//
			}

			pthread_mutex_destroy(&pool->mutex);
		}

		errno = ret;
		msgqueue_destroy(pool->msgqueue);
	}

	free(pool);
	return NULL;
}

inline void __thrdpool_schedule(const struct thrdpool_task *task, void *buf,
								thrdpool_t *pool);//?为啥没有实现，内联？

void __thrdpool_schedule(const struct thrdpool_task *task, void *buf,//重名？，buf是msgqueue中的entry，把task传给entry，往msgqueue put 任务
						 thrdpool_t *pool)
{
	((struct __thrdpool_task_entry *)buf)->task = *task;
	msgqueue_put(buf, pool->msgqueue);//往消息队列里放task
}

int thrdpool_schedule(const struct thrdpool_task *task, thrdpool_t *pool)
{
	void *buf = malloc(sizeof (struct __thrdpool_task_entry));

	if (buf)
	{
		__thrdpool_schedule(task, buf, pool);
		return 0;
	}

	return -1;
}

int thrdpool_increase(thrdpool_t *pool)
{
	pthread_attr_t attr;
	pthread_t tid;
	int ret;

	ret = pthread_attr_init(&attr);
	if (ret == 0)
	{
		if (pool->stacksize)
			pthread_attr_setstacksize(&attr, pool->stacksize);

		pthread_mutex_lock(&pool->mutex);
		ret = pthread_create(&tid, &attr, __thrdpool_routine, pool);//线程池增加一个线程
		if (ret == 0)
			pool->nthreads++;

		pthread_mutex_unlock(&pool->mutex);
		pthread_attr_destroy(&attr);
		if (ret == 0)
			return 0;
	}

	errno = ret;
	return -1;
}

inline int thrdpool_in_pool(thrdpool_t *pool);

int thrdpool_in_pool(thrdpool_t *pool)
{
	return pthread_getspecific(pool->key) == pool;//判断调用这个方法的线程是否在参数pool里
}

void thrdpool_destroy(void (*pending)(const struct thrdpool_task *),
					  thrdpool_t *pool)
{
	int in_pool = thrdpool_in_pool(pool);
	struct __thrdpool_task_entry *entry;

	__thrdpool_terminate(in_pool, pool);//销毁所有线程
	while (1)
	{
		entry = (struct __thrdpool_task_entry *)msgqueue_get(pool->msgqueue);
		if (!entry)//没有任务了
			break;

		if (pending)//还有任务
			pending(&entry->task);//pending是干啥的？一个函数指针，作用于线程池线程销毁了，但是消息队列中还没有运行的任务，具体干啥看传的具体函数

		free(entry);
	}

	pthread_key_delete(pool->key);
	pthread_mutex_destroy(&pool->mutex);
	msgqueue_destroy(pool->msgqueue);
	if (!in_pool)
		free(pool);
}

