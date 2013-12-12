#pragma once

/****************************************************************************
* Wait-Free MPSC Queue
* Minimum requirement: Windows XP or Windows Server 2003
* Author: @sm9kr
* License type: GPL v3
* References
** [1] http://www.boost.org/doc/libs/1_35_0/doc/html/intrusive/intrusive_vs_nontrusive.html
** [2] http://groups.google.ru/group/comp.programming.threads/browse_frm/thread/33f79c75146582f3
****************************************************************************/


/// 노드 구조
class NodeEntry
{
public:
	NodeEntry() : mNext(nullptr) {}
	NodeEntry* volatile mNext;
};

/// 아래와 같은 형태로 intrusive방식으로 노드 구성해야 함 [1]
class DataNode
{
public:
	DataNode() {}

	__int64		mData;
	NodeEntry	mNodeEntry; ///< 반드시 NodeEntry를 포함해야 함
};


/**
* [2]을 참고하여 C++ Windows 환경에 맞게 변경한 MPSC 큐.
* 여러 쓰레드에서 Push는 되지만 Pop은 하나의 지정된 쓰레드에서만 해야 함
* 사용 예
	WaitFreeQueue<DataNode> testQueue ;
	DataNode* pushData = new DataNode ;
	testQueue.Push(newData) ;
	DataNode* popData = testQueue.Pop() ;
	delete popData ;
* 물론, DataNode*가 큐 안에 있을때 다른 쓰레드에서 날려버지 않도록 스마트 포인터로 만들어서 쓰는 것이 좋다.
*/

template <class T>
class WaitFreeQueue
{
public:
	WaitFreeQueue() : mHead(&mStub), mTail(&mStub)
	{
		mOffset = reinterpret_cast<__int64>(&((reinterpret_cast<T*>(0))->mNodeEntry));
	}
	~WaitFreeQueue() {}

	void Push(T* newData)
	{
		NodeEntry* prevNode = (NodeEntry*)InterlockedExchangePointer((void*)&mHead, (void*)&(newData->mNodeEntry));
		prevNode->mNext = &(newData->mNodeEntry);
	}

	T* Pop()
	{
		NodeEntry* tail = mTail;
		NodeEntry* next = tail->mNext;

		if (tail == &mStub)
		{
			/// 데이터가 없을 때
			if (nullptr == next)
				return nullptr;

			/// 처음 꺼낼 때
			mTail = next;
			tail = next;
			next = next->mNext;
		}

		/// 대부분의 경우에 데이터를 빼낼 때
		if (next)
		{
			mTail = next;

			return reinterpret_cast<T*>(reinterpret_cast<__int64>(tail)-mOffset);
		}

		NodeEntry* head = mHead;
		if (tail != head)
			return nullptr;

		/// 마지막 데이터 꺼낼 때
		mStub.mNext = nullptr;
		NodeEntry* prev = (NodeEntry*)InterlockedExchangePointer((void*)&mHead, (void*)&mStub);
		prev->mNext = &mStub;

		next = tail->mNext;
		if (next)
		{
			mTail = next;

			return reinterpret_cast<T*>(reinterpret_cast<__int64>(tail)-mOffset);
		}

		return nullptr;
	}


private:

	NodeEntry* volatile	mHead;
	NodeEntry*			mTail;
	NodeEntry			mStub;

	__int64				mOffset;

};
