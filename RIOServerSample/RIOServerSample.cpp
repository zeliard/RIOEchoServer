/*********************************************************************************************************
* Windows Registered I/O (RIO) Sample Code (Echo Server)
* Minimum requirement: Windows 8 or Windows Server 2012
* Author: @sm9kr
*
* Notice
*  AllocateBufferSpace() from http://www.serverframework.com/asynchronousevents/rio/
*********************************************************************************************************/

#include "stdafx.h"

#pragma comment(lib, "ws2_32.lib")


RIO_EXTENSION_FUNCTION_TABLE g_rio;
RIO_CQ g_completionQueue = 0;
RIO_RQ g_requestQueue = 0;

HANDLE g_hIOCP = NULL;

SOCKET g_socket;

CRITICAL_SECTION g_criticalSection;

RIO_BUFFERID g_sendBufferId;
RIO_BUFFERID g_recvBufferId;
RIO_BUFFERID g_addrBufferId;

char* g_sendBufferPointer = NULL;
char* g_recvBufferPointer = NULL;
char* g_addrBufferPointer = NULL;


static const unsigned short PORTNUM = 5050;

static const DWORD RIO_PENDING_RECVS = 100000;
static const DWORD RIO_PENDING_SENDS = 10000;

static const DWORD RECV_BUFFER_SIZE = 1024;
static const DWORD SEND_BUFFER_SIZE = 1024;

static const DWORD ADDR_BUFFER_SIZE = 64;

static const DWORD NUM_IOCP_THREADS = 4;

static const DWORD RIO_MAX_RESULTS = 1000;

HANDLE g_threads[NUM_IOCP_THREADS] = { NULL, };


enum COMPLETION_KEY
{
	CK_STOP = 0,
	CK_START = 1
};

enum OPERATION_TYPE
{
	OP_NONE = 0,
	OP_RECV = 1,
	OP_SEND = 2
};

struct EXTENDED_RIO_BUF : public RIO_BUF
{
	OPERATION_TYPE operation;
};


/// RIO_BUF for SEND (circular)
EXTENDED_RIO_BUF* g_sendRioBufs = NULL;
DWORD g_sendRioBufTotalCount = 0;
__int64 g_sendRioBufIndex = 0;

/// RIO_BUF for ADDR (circular)
EXTENDED_RIO_BUF* g_addrRioBufs = NULL;
DWORD g_addrRioBufTotalCount = 0;
__int64 g_addrRioBufIndex = 0;

char* AllocateBufferSpace(const DWORD bufSize, const DWORD bufCount, DWORD& totalBufferSize, DWORD& totalBufferCount);
unsigned int __stdcall IOThread(void *pV);

int _tmain(int argc, _TCHAR* argv[])
{
	WSADATA data;
	InitializeCriticalSectionAndSpinCount(&g_criticalSection, 4000);

	if (0 != ::WSAStartup(0x202, &data))
	{
			printf_s("WSAStartup Error: %d\n", GetLastError());
		exit(0);
	}

	/// RIO socket
	g_socket = WSASocket(AF_INET, SOCK_DGRAM, IPPROTO_UDP, NULL, 0, WSA_FLAG_REGISTERED_IO);
	if (g_socket == INVALID_SOCKET)
	{
		printf_s("WSASocket Error: %d\n", GetLastError());
		exit(0);
	}

	/// port binding
	sockaddr_in addr;
	addr.sin_family = AF_INET;
	addr.sin_port = htons(PORTNUM);
	addr.sin_addr.s_addr = INADDR_ANY;

	if (SOCKET_ERROR == ::bind(g_socket, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)))
	{
		printf_s("Bind Error: %d\n", GetLastError());
		exit(0);
	}

	/// RIO function table
	GUID functionTableId = WSAID_MULTIPLE_RIO;
	DWORD dwBytes = 0;

	if (NULL != WSAIoctl(g_socket, SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER, &functionTableId, sizeof(GUID),
		(void**)&g_rio, sizeof(g_rio), &dwBytes, NULL, NULL))
	{
		printf_s("WSAIoctl Error: %d\n", GetLastError());
		exit(0);
	}

	/// rio's completion manner: iocp
	g_hIOCP = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, 0, 0, 0);
	if (NULL == g_hIOCP)
	{
		printf_s("CreateIoCompletionPort Error: %d\n", GetLastError());
		exit(0);
	}


	OVERLAPPED overlapped;

	RIO_NOTIFICATION_COMPLETION completionType;

	completionType.Type = RIO_IOCP_COMPLETION;
	completionType.Iocp.IocpHandle = g_hIOCP;
	completionType.Iocp.CompletionKey = (void*)CK_START;
	completionType.Iocp.Overlapped = &overlapped;

	/// creating RIO CQ, which is bigger than (or equal to) RQ size
	g_completionQueue = g_rio.RIOCreateCompletionQueue(RIO_PENDING_RECVS + RIO_PENDING_SENDS, &completionType);
	if (g_completionQueue == RIO_INVALID_CQ)
	{
		printf_s("RIOCreateCompletionQueue Error: %d\n", GetLastError());
		exit(0);
	}

	/// creating RIO RQ
	/// SEND and RECV within one CQ (you can do with two CQs, seperately)
	g_requestQueue = g_rio.RIOCreateRequestQueue(g_socket, RIO_PENDING_RECVS, 1, RIO_PENDING_SENDS, 1, g_completionQueue, g_completionQueue, NULL);
	if (g_requestQueue == RIO_INVALID_RQ)
	{
		printf_s("RIOCreateRequestQueue Error: %d\n", GetLastError());
		exit(0);
	}


	/// registering RIO buffers for SEND
	{
		DWORD totalBufferCount = 0;
		DWORD totalBufferSize = 0;

		g_sendBufferPointer = AllocateBufferSpace(SEND_BUFFER_SIZE, RIO_PENDING_SENDS, totalBufferSize, totalBufferCount);
		g_sendBufferId = g_rio.RIORegisterBuffer(g_sendBufferPointer, static_cast<DWORD>(totalBufferSize));

		if (g_sendBufferId == RIO_INVALID_BUFFERID)
		{
			printf_s("RIORegisterBuffer Error: %d\n", GetLastError());
			exit(0);
		}

		DWORD offset = 0;

		g_sendRioBufs = new EXTENDED_RIO_BUF[totalBufferCount];
		g_sendRioBufTotalCount = totalBufferCount;

		for (DWORD i = 0; i < g_sendRioBufTotalCount; ++i)
		{
			/// split g_sendRioBufs to SEND_BUFFER_SIZE for each RIO operation
			EXTENDED_RIO_BUF* pBuffer = g_sendRioBufs + i;

			pBuffer->operation = OP_SEND;
			pBuffer->BufferId = g_sendBufferId;
			pBuffer->Offset = offset;
			pBuffer->Length = SEND_BUFFER_SIZE;

			offset += SEND_BUFFER_SIZE;

		}

	}


	/// registering RIO buffers for ADDR
	{
		DWORD totalBufferCount = 0;
		DWORD totalBufferSize = 0;

		g_addrBufferPointer = AllocateBufferSpace(ADDR_BUFFER_SIZE, RIO_PENDING_RECVS, totalBufferSize, totalBufferCount);
		g_addrBufferId = g_rio.RIORegisterBuffer(g_addrBufferPointer, static_cast<DWORD>(totalBufferSize));

		if (g_addrBufferId == RIO_INVALID_BUFFERID)
		{
			printf_s("RIORegisterBuffer Error: %d\n", GetLastError());
			exit(0);
		}

		DWORD offset = 0;

		g_addrRioBufs = new EXTENDED_RIO_BUF[totalBufferCount];
		g_addrRioBufTotalCount = totalBufferCount;

		for (DWORD i = 0; i < totalBufferCount; ++i)
		{
			EXTENDED_RIO_BUF* pBuffer = g_addrRioBufs + i;

			pBuffer->operation = OP_NONE;
			pBuffer->BufferId = g_addrBufferId;
			pBuffer->Offset = offset;
			pBuffer->Length = ADDR_BUFFER_SIZE;

			offset += ADDR_BUFFER_SIZE;
		}
	}

	/// registering RIO buffers for RECV and then, post pre-RECV
	{
		DWORD totalBufferCount = 0;
		DWORD totalBufferSize = 0;

		g_recvBufferPointer = AllocateBufferSpace(RECV_BUFFER_SIZE, RIO_PENDING_RECVS, totalBufferSize, totalBufferCount);

		g_recvBufferId = g_rio.RIORegisterBuffer(g_recvBufferPointer, static_cast<DWORD>(totalBufferSize));
		if (g_recvBufferId == RIO_INVALID_BUFFERID)
		{
			printf_s("RIORegisterBuffer Error: %d\n", GetLastError());
			exit(0);
		}


		DWORD offset = 0;

		EXTENDED_RIO_BUF* pBufs = new EXTENDED_RIO_BUF[totalBufferCount];

		for (DWORD i = 0; i < totalBufferCount; ++i)
		{
			EXTENDED_RIO_BUF* pBuffer = pBufs + i;

			pBuffer->operation = OP_RECV;
			pBuffer->BufferId = g_recvBufferId;
			pBuffer->Offset = offset;
			pBuffer->Length = RECV_BUFFER_SIZE;

			offset += RECV_BUFFER_SIZE;

			/// posting pre RECVs
			if (!g_rio.RIOReceiveEx(g_requestQueue, pBuffer, 1, NULL, &g_addrRioBufs[g_addrRioBufIndex++], NULL, 0, 0, pBuffer))
			{
				printf_s("RIOReceive Error: %d\n", GetLastError());
				exit(0);
			}
		}

		printf_s("%d total receives pending\n", totalBufferCount);
	}


	/// creating IO threads
	for (DWORD i = 0; i < NUM_IOCP_THREADS; ++i)
	{
		unsigned int notUsed;
		const uintptr_t result = ::_beginthreadex(0, 0, IOThread, (void*)i, 0, &notUsed);
		if (result == 0)
		{
			printf_s("_beginthreadex Error: %d\n", GetLastError());
			exit(0);
		}

		g_threads[i] = (HANDLE)result;
	}

	/// notify completion-ready 
	INT notifyResult = g_rio.RIONotify(g_completionQueue);
	if (notifyResult != ERROR_SUCCESS)
	{
		printf_s("RIONotify Error: %d\n", GetLastError());
		exit(0);
	}


	/// terminating i/o to press any key 
	printf_s("Press Any Key to Stop\n");
	getchar();



	/// completion key CK_STOP: to stop I/O threads
	for (DWORD i = 0; i < NUM_IOCP_THREADS; ++i)
	{
		if (0 == ::PostQueuedCompletionStatus(g_hIOCP, 0, CK_STOP, 0))
		{
			printf_s("PostQueuedCompletionStatus Error: %d\n", GetLastError());
			exit(0);
		}
	}

	/// thread join
	for (DWORD i = 0; i < NUM_IOCP_THREADS; ++i)
	{
		HANDLE hThread = g_threads[i];

		if (WAIT_OBJECT_0 != ::WaitForSingleObject(hThread, INFINITE))
		{	
			printf_s("WaitForSingleObject (thread join) Error: %d\n", GetLastError());
			exit(0);
		}

		::CloseHandle(hThread);
	}


	if (SOCKET_ERROR == ::closesocket(g_socket))
	{
		printf_s("closesocket Error: %d\n", GetLastError());
	}

	g_rio.RIOCloseCompletionQueue(g_completionQueue);

	g_rio.RIODeregisterBuffer(g_sendBufferId);
	g_rio.RIODeregisterBuffer(g_recvBufferId);
	g_rio.RIODeregisterBuffer(g_addrBufferId);

	DeleteCriticalSection(&g_criticalSection);

	return 0;
}


unsigned int __stdcall IOThread(void *pV)
{
	DWORD numberOfBytes = 0;

	ULONG_PTR completionKey = 0;
	OVERLAPPED* pOverlapped = 0;

	RIORESULT results[RIO_MAX_RESULTS];

	while (true)
	{
		if (!::GetQueuedCompletionStatus(g_hIOCP, &numberOfBytes, &completionKey, &pOverlapped, INFINITE))
		{
			printf_s("GetQueuedCompletionStatus Error: %d\n", GetLastError());
			exit(0);
		}

		/// exit when CK_STOP
		if (completionKey == CK_STOP)
			break;

		memset(results, 0, sizeof(results));

		ULONG numResults = g_rio.RIODequeueCompletion(g_completionQueue, results, RIO_MAX_RESULTS);
		if (0 == numResults || RIO_CORRUPT_CQ == numResults)
		{
			printf_s("RIODequeueCompletion Error: %d\n", GetLastError());
			exit(0);
		}

		/// Notify after Dequeueing
		INT notifyResult = g_rio.RIONotify(g_completionQueue);
		if (notifyResult != ERROR_SUCCESS)
		{
			printf_s("RIONotify Error: %d\n", GetLastError());
			exit(0);
		}

		for (DWORD i = 0; i < numResults; ++i)
		{
			EXTENDED_RIO_BUF* pBuffer = reinterpret_cast<EXTENDED_RIO_BUF*>(results[i].RequestContext);

			if (OP_RECV == pBuffer->operation)
			{
				/// error when total packet is not arrived because this is UDP
				if (results[i].BytesTransferred != RECV_BUFFER_SIZE)
					break;

				///// ECHO TEST
				const char* offset = g_recvBufferPointer + pBuffer->Offset;

				/// RQ is not thread-safe (need to be optimized...)
				::EnterCriticalSection(&g_criticalSection);
				{

					EXTENDED_RIO_BUF* sendBuf = &(g_sendRioBufs[g_sendRioBufIndex++ % g_sendRioBufTotalCount]);
					char* sendOffset = g_sendBufferPointer + sendBuf->Offset;
					memcpy_s(sendOffset, RECV_BUFFER_SIZE, offset, pBuffer->Length);

			

					if (!g_rio.RIOSendEx(g_requestQueue, sendBuf, 1, NULL, &g_addrRioBufs[g_addrRioBufIndex % g_addrRioBufTotalCount], NULL, NULL, 0, sendBuf))
					{
						printf_s("RIOSend Error: %d\n", GetLastError());
						exit(0);
					}

				}
				::LeaveCriticalSection(&g_criticalSection);


			}
			else if (OP_SEND == pBuffer->operation)
			{
				/// RQ is not thread-safe
				::EnterCriticalSection(&g_criticalSection);
				{
					if (!g_rio.RIOReceiveEx(g_requestQueue, pBuffer, 1, NULL, &g_addrRioBufs[g_addrRioBufIndex % g_addrRioBufTotalCount], NULL, 0, 0, pBuffer))
					{
						printf_s("RIOReceive Error: %d\n", GetLastError());
						exit(0);;
					}

					g_addrRioBufIndex++;

				}
				::LeaveCriticalSection(&g_criticalSection);

			}
			else
				break;

		}
	}


	return 0;
}


template <typename TV, typename TM>
inline TV RoundDown(TV Value, TM Multiple)
{
	return((Value / Multiple) * Multiple);
}

template <typename TV, typename TM>
inline TV RoundUp(TV Value, TM Multiple)
{
	return(RoundDown(Value, Multiple) + (((Value % Multiple) > 0) ? Multiple : 0));
}


char* AllocateBufferSpace(const DWORD bufSize, const DWORD bufCount, DWORD& totalBufferSize, DWORD& totalBufferCount)
{
	SYSTEM_INFO systemInfo;

	::GetSystemInfo(&systemInfo);

	const unsigned __int64 granularity = systemInfo.dwAllocationGranularity;

	const unsigned __int64 desiredSize = bufSize * bufCount;

	unsigned __int64 actualSize = RoundUp(desiredSize, granularity);

	if (actualSize > UINT_MAX )
	{
		actualSize = (UINT_MAX / granularity) * granularity;
	}

	totalBufferCount =  min(bufCount, static_cast<DWORD>(actualSize / bufSize));

	totalBufferSize = static_cast<DWORD>(actualSize);

	char* pBuffer = reinterpret_cast<char*>(VirtualAllocEx(GetCurrentProcess(), 0, totalBufferSize, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE));

	if (pBuffer == 0)
	{
		printf_s("VirtualAllocEx Error: %d\n", GetLastError());
		exit(0);
	}

	return pBuffer;
}