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
#include <stdlib.h>
#include <pthread.h>
#include "list.h"
#include "thrdpool.h"
#include "Executor.h"

struct ExecTaskEntry
{
	struct list_head list;//
	ExecSession *session;//所属的session
	thrdpool_t *thrdpool;//执行的线程池
};

int ExecQueue::init()//里面有任务链表
{
	int ret;

	ret = pthread_mutex_init(&this->mutex, NULL);
	if (ret == 0)
	{
		INIT_LIST_HEAD(&this->task_list);//这里很重要，看下task_list最开始是什么样的
		return 0;
	}

	errno = ret;
	return -1;
}

void ExecQueue::deinit()
{
	pthread_mutex_destroy(&this->mutex);
}

int Executor::init(size_t nthreads)//里面有线程池
{
	if (nthreads == 0)
	{
		errno = EINVAL;
		return -1;
	}

	this->thrdpool = thrdpool_create(nthreads, 0);//stacksize=0?
	if (this->thrdpool)
		return 0;

	return -1;
}

void Executor::deinit()
{
	thrdpool_destroy(Executor::executor_cancel_tasks, this->thrdpool);
}

extern "C" void __thrdpool_schedule(const struct thrdpool_task *, void *,
									thrdpool_t *);//？？？？？？？？？？？？？？？？？？？让c++ 兼容c代码，这里的代码要用c编译器编译（因为编译规则不一样，翻译成符号的时候的规则不一样吧）

void Executor::executor_thread_routine(void *context)
{
	ExecQueue *queue = (ExecQueue *)context;
	struct ExecTaskEntry *entry;
	ExecSession *session;

	pthread_mutex_lock(&queue->mutex);
	entry = list_entry(queue->task_list.next, struct ExecTaskEntry, list);//这是宏函数，list相当于一个字符串，预编译翻译宏的时候，会翻译成宏函数的定义，具体做的就是，通过list_head这个结构体中的next指针，通过偏移（后面两个参数算出来的偏移），找到包含next list结构体的ExecTaskEntry结构体
	//task_list.next是双向链表的头部，相当于先进先出，因为从尾部add，从头部开始执行
	list_del(&entry->list);//相当于把list中的task所属的ExecTaskEntry取出来了，就可以把这个task，从list中去除，这个list保证前后必须有list，那只有两个list的情况呢？
	//看懂了，假如queue有单个节点，但task_list尾节点知识起到定位作用，因此只有两个任务去执行，第一次进入这个方法，先删除，这个时候只有两个节点，检查不为空，发送msg后，执行下面的session->execute();/及执行了一次任务，
	//刚才那个msg，被其他线程执行后，又进入这个方法，再del一次，然后只剩下task_list节点了（task_list节点的next是自己，因此判断为空），下面判断跳过，直接执行exectue方法，即总共运行了两次任务
	session = entry->session;
	if (!list_empty(&queue->task_list))
	{
		struct thrdpool_task task = {
			.routine	=	Executor::executor_thread_routine,//递归了，重点在这里
			.context	=	queue
		};
		__thrdpool_schedule(&task, entry, entry->thrdpool);//拿到任务，放到线程池中执行，entry即之前的msg，里面有task和link，这里即设置了task
	}
	else
		free(entry);

	pthread_mutex_unlock(&queue->mutex);
	session->execute();//这里是真正执行的内容，每一个entry都有一个session，对应一个任务，最开始的时候，主线程第一次执行这个任务，然后
	//因为执行之前，又创建了一个task，所以，他会把这个task放进消息队列，通过主线程，然后，线程池中另外一个线程取到那个task，然后继续执行，相当于再次执行executor_thread_routine
	//但此时是另一个线程了，然后再一次创建task，然后放进消息队列，放完之后就返回，然后执行session->execute()，以此类推，直到执行完所有的session任务（每次session，从entry获得），所以整个queue任务链表，不是依次执行的，相当于并行执行的
	session->handle(ES_STATE_FINISHED, 0);
}

void Executor::executor_cancel_tasks(const struct thrdpool_task *task)//这个还没看
{
	ExecQueue *queue = (ExecQueue *)task->context;
	struct ExecTaskEntry *entry;
	struct list_head *pos, *tmp;
	ExecSession *session;

	list_for_each_safe(pos, tmp, &queue->task_list)
	{
		entry = list_entry(pos, struct ExecTaskEntry, list);
		list_del(pos);
		session = entry->session;
		free(entry);

		session->handle(ES_STATE_CANCELED, 0);
	}
}

int Executor::request(ExecSession *session, ExecQueue *queue)//request的含义是，往ExecQueue中放入一个entry，以及entry中的session（session中的execute是具体的执行任务）
{
	struct ExecTaskEntry *entry;

	session->queue = queue;//把这个任务分配到这个ExecQueue中
	entry = (struct ExecTaskEntry *)malloc(sizeof (struct ExecTaskEntry));
	if (entry)
	{
		entry->session = session;//entry放到session中
		entry->thrdpool = this->thrdpool;//entry分配一个thrdpool
		pthread_mutex_lock(&queue->mutex);
		list_add_tail(&entry->list, &queue->task_list);//Execquue中有一个task_list指针，其实是个双向链表，把task_list理解成双向链表的表尾部，在尾部前面增加节点，task_list 的next是双向链表的头部
		if (queue->task_list.next == &entry->list)
		{
			struct thrdpool_task task = {
				.routine	=	Executor::executor_thread_routine,
				.context	=	queue
			};
			if (thrdpool_schedule(&task, this->thrdpool) < 0)
			{
				list_del(&entry->list);
				free(entry);
				entry = NULL;
			}
		}

		pthread_mutex_unlock(&queue->mutex);
	}

	return -!entry;
}

