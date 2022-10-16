/*
  Copyright (c) 2020 Sogou, Inc.

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

/*
 * This message queue originates from the project of Sogou C++ Workflow:
 * https://github.com/sogou/workflow
 *
 * The idea of this implementation is quite simple and obvious. When the
 * get_list is not empty, the consumer takes a message. Otherwise the consumer
 * waits till put_list is not empty, and swap two lists. This method performs
 * well when the queue is very busy, and the number of consumers is big.
 */

#include <errno.h>
#include <stdlib.h>
#include <pthread.h>
#include "msgqueue.h"

struct __msgqueue
{
	size_t msg_max;
	size_t msg_cnt;
	int linkoff;//创建msg的时候，只有一个成员变量，把linkoff处多申请一个指针的内存，作为next指针，而不是再加一个成员变量？其实不是很理解？看看用的时候msg怎么创建的？
	int nonblock;
	void *head1;//队列1（头部指针），可能为生产者队列，也可能为消费者队列，会交换的，取决于哪种二重指针指向他们
	void *head2;//队列2
	void **get_head;//消费者队列，指向消费者队列头部指针
	void **put_head;//生产者队列，指向生产者头部指针
	void **put_tail;//生产者队列，指向生产者尾部指针
	pthread_mutex_t get_mutex;//消费者锁
	pthread_mutex_t put_mutex;
	pthread_cond_t get_cond;//消费者条件变量，需配合锁使用，用于线程间通信（通过全局变量）
	pthread_cond_t put_cond;
};

void msgqueue_set_nonblock(msgqueue_t *queue)
{
	queue->nonblock = 1;
	pthread_mutex_lock(&queue->put_mutex);//参数为put_mutex的地址，应该会隐式转换为pthread_mutex_t *__mutex指针
	pthread_cond_signal(&queue->get_cond);
	pthread_cond_broadcast(&queue->put_cond);
	pthread_mutex_unlock(&queue->put_mutex);
}

void msgqueue_set_block(msgqueue_t *queue)
{
	queue->nonblock = 0;
}

static size_t __msgqueue_swap(msgqueue_t *queue)
{
	void **get_head = queue->get_head;//执行到这里，说明消费者队列已经没有msg了
	size_t cnt;

	queue->get_head = queue->put_head;
	pthread_mutex_lock(&queue->put_mutex);
	while (queue->msg_cnt == 0 && !queue->nonblock)//生产者队列为0，需要收到生产者生产新msg的信号，再进行后续操作
		pthread_cond_wait(&queue->get_cond, &queue->put_mutex);//会先释放put_mutex锁，然后阻塞等待，直到被激活（激活主动方会主动释放锁），重新获取put_mutex锁

	cnt = queue->msg_cnt;//生产者生产的msg个数
	if (cnt > queue->msg_max - 1)//假如生产队列的msg数量大于到达最大数量（说明之前生产的太快，到达max，生产者卡在93行了，swap后需要重新生产）
		pthread_cond_broadcast(&queue->put_cond);//会通知等待put_cond条件变量的生产者准备去生产（因为交换后生产者就变为0），但是还没有释放锁，所以生产者还不能生产，即93行

	queue->put_head = get_head;//因为交换前消费者队列为空，因此，put_head和put_tail都为get_head指针
	queue->put_tail = get_head;
	queue->msg_cnt = 0;//已经进行了交换，生产者队列msg为0
	pthread_mutex_unlock(&queue->put_mutex);//终于释放了锁，93行会获取锁，然后继续生产
	return cnt;
}

void msgqueue_put(void *msg, msgqueue_t *queue)
{
	void **link = (void **)((char *)msg + queue->linkoff);//这里明明是指针在msg后面，但是struct __thrdpool_task_entry//先是next指针，后是数据本体

	*link = NULL;//link是指向指针的指针，提解一次变成它指向的那个指针，令void*指针为NULL
	pthread_mutex_lock(&queue->put_mutex);
	while (queue->msg_cnt > queue->msg_max - 1 && !queue->nonblock)//如果生产者队列已满，while是因为防止下面误唤醒，是误唤醒的话，会再次wait
		pthread_cond_wait(&queue->put_cond, &queue->put_mutex);//收到put的通知，参会继续执行下面的代码

	*queue->put_tail = link;//重点
	queue->put_tail = link;
	queue->msg_cnt++;
	pthread_mutex_unlock(&queue->put_mutex);
	pthread_cond_signal(&queue->get_cond);
}

void *msgqueue_get(msgqueue_t *queue)
{
	void *msg;//创建空消息指针

	pthread_mutex_lock(&queue->get_mutex);//获取get队列的锁（锁是引用）
	if (*queue->get_head || __msgqueue_swap(queue) > 0)//get队列有msg，或者没有msg，但是put队列和get队列交换了，返回值是get队列的msg个数
	{
		msg = (char *)*queue->get_head - queue->linkoff;
		*queue->get_head = *(void **)*queue->get_head;
	}
	else
	{
		msg = NULL;
		errno = ENOENT;
	}

	pthread_mutex_unlock(&queue->get_mutex);
	return msg;
}

msgqueue_t *msgqueue_create(size_t maxlen, int linkoff)//不懂
{
	msgqueue_t *queue = (msgqueue_t *)malloc(sizeof (msgqueue_t));
	int ret;

	if (!queue)
		return NULL;

	ret = pthread_mutex_init(&queue->get_mutex, NULL);//get_mutex是个struct中的变量，按理说这个变量还没有初始值的吧
	if (ret == 0)
	{
		ret = pthread_mutex_init(&queue->put_mutex, NULL);
		if (ret == 0)
		{
			ret = pthread_cond_init(&queue->get_cond, NULL);
			if (ret == 0)
			{
				ret = pthread_cond_init(&queue->put_cond, NULL);//init和下面的destroy对称
				if (ret == 0)
				{
					queue->msg_max = maxlen;
					queue->linkoff = linkoff;
					queue->head1 = NULL;
					queue->head2 = NULL;
					queue->get_head = &queue->head1;//初始化时，head1为生产消费者队列
					queue->put_head = &queue->head2;
					queue->put_tail = &queue->head2;
					queue->msg_cnt = 0;
					queue->nonblock = 0;
					return queue;
				}

				pthread_cond_destroy(&queue->get_cond);
			}

			pthread_mutex_destroy(&queue->put_mutex);
		}

		pthread_mutex_destroy(&queue->get_mutex);
	}

	errno = ret;
	free(queue);
	return NULL;
}

void msgqueue_destroy(msgqueue_t *queue)
{
	pthread_cond_destroy(&queue->put_cond);
	pthread_cond_destroy(&queue->get_cond);
	pthread_mutex_destroy(&queue->put_mutex);
	pthread_mutex_destroy(&queue->get_mutex);
	free(queue);
}

