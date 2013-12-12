#pragma once

/****************************************************************************
* Wait-Free MPMC Queue
* Minimum requirement: Windows XP or Windows Server 2003
* Author: @sm9kr
* License type: GPL v3
* Drawback: Fixed size, __int64 overflow-able
****************************************************************************/

/// 멀티쓰레드 환경에서 아주 빠르게 (wait-free) FIFO 가능함
/// 단, 고정크기라서, 큐가 꽉차면 뻑남 (쓰고자 하는 어플리케이션 특성에 맞게 적당히 큰 크기로 조정할 것)
/// 인덱스(head, tail)가 2^63을 넘어서면 뻑남 (이건 현실적으로는 일어나기 힘들긴 함)


#define _ASSERTC(expr) \
	{ \
if (!(expr)) \
		{ \
		int* dummy = 0; \
		*dummy = 0xDEADBEEF; \
		} \
	}



template <class T>
class MPMCQueue
{
public:

	template<int E>
	struct PowerOfTwo
	{
		enum { value = 2 * PowerOfTwo<E - 1>::value };
	};

	template<>
	struct PowerOfTwo<0>
	{
		enum { value = 1 };
	};

	enum
	{
		/// 큐의 크기 설정: 반드시 2의 승수로 해야 한다.
		QUEUE_MAX_SIZE = PowerOfTwo<16>::value,
		QUEUE_SIZE_MASK = QUEUE_MAX_SIZE - 1
	};

	MPMCQueue() : mHeadPos(0), mTailPos(0)
	{
		memset(mElem, 0, sizeof(mElem));
	}

	void Push(T* newElem)
	{
		__int64 insertPos = InterlockedIncrement64(&mTailPos) - 1;
		_ASSERTC(insertPos - mHeadPos < QUEUE_MAX_SIZE); ///< overflow

		mElem[insertPos&QUEUE_SIZE_MASK] = newElem;
	}

	T* Pop()
	{
		T* popVal = (T*)InterlockedExchangePointer((void**)&mElem[mHeadPos&QUEUE_SIZE_MASK], nullptr);

		if (popVal != nullptr)
			InterlockedIncrement64(&mHeadPos);

		return popVal;
	}

	__int64 GetSize() const
	{
		return mTailPos - mHeadPos;
	}



private:

	T*			mElem[QUEUE_MAX_SIZE];
	volatile __int64	mHeadPos;
	volatile __int64	mTailPos;

};
