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

#ifndef _SUBTASK_H_
#define _SUBTASK_H_

#include <stddef.h>

class ParallelTask;//前置声明，下面的类中如果有这个成员变量，则必须是指针。这里用到前置声明，最主要是不是为了防止循环（类成员是自己的子类，如果把ParallelTask类的定义放这，因为继承自SubTask，所以SubTask也要前置声明）

class SubTask
{
public:
	virtual void dispatch() = 0;

private:
	virtual SubTask *done() = 0;

protected:
	void subtask_done();

public:
	ParallelTask *get_parent_task() const { return this->parent; }//没有用到ParallelTask内部的东西，因此可以使用前置声明的方式，减少了类的大小（因为成员变量为声明类的指针，不是声明类本身）
	void *get_pointer() const { return this->pointer; }
	void set_pointer(void *pointer) { this->pointer = pointer; }

private:
	ParallelTask *parent;//必须是指针，如果不是指针，因为前置声明没有类的定义，所以并不知道类的大小，则分配空间不知道分配多少，但是指针的话，知道大小，subtask类就可以进行创建对象了
	SubTask **entry;
	void *pointer;

public:
	SubTask()
	{
		this->parent = NULL;
		this->entry = NULL;
		this->pointer = NULL;
	}

	virtual ~SubTask() { }
	friend class ParallelTask;
};

class ParallelTask : public SubTask
{
public:
	ParallelTask(SubTask **subtasks, size_t n)
	{
		this->subtasks = subtasks;
		this->subtasks_nr = n;
	}

	SubTask **get_subtasks(size_t *n) const
	{
		*n = this->subtasks_nr;
		return this->subtasks;
	}

	void set_subtasks(SubTask **subtasks, size_t n)
	{
		this->subtasks = subtasks;
		this->subtasks_nr = n;
	}

public:
	virtual void dispatch();

protected:
	SubTask **subtasks;
	size_t subtasks_nr;

private:
	size_t nleft;
	friend class SubTask;
};

#endif

