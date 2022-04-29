from twisted.internet import defer

TARGET = 1000000

def largeFibonnaciNumber():
    d = defer.Deferred()

    first = 0
    second = 1

    for i in range(TARGET-1):
        new = first + second
        first = second
        second = new

        # if i % 100 == 0:
        #     print(f"calculating the {i}th Fibonnaci number")

    d.callback(second)

    return d

import time

start_time = time.time()
d = largeFibonnaciNumber()
end_time = time.time()
print(f"spend time {end_time-start_time}")


def printNumber(num):
    print(f"the {TARGET}th Fibonacci number is ***")


print("Adding the callback now")
d.addCallback(printNumber)

"""期望的运行方式：（异步形式，如果计算结果足够大，将会消耗更长的时间，此举为模拟出阻塞的操作）
1、异步运行斐波那契数列的计算（长阻塞操作）
2、马上输出spend time，因为是异步处理
3、马上输出Adding the callback now
4、最后返回第n-1次计算的值
"""


"""
代码没有预期地执行异步，从结果上来看，是先计算完斐波那契数列，再输出时间，最后才触发回调，整体看起来就像是同步的操作
callback并没有在结果返回前添加到deferred对象中，也没有在结果返回时调用，甚至在计算完成之后它也没有添加到deferred对象中。

函数在返回之前已经完成了计算，计算阻塞了当前进程直到完成，这是异步代码不会做的。因此Deferred不是非阻塞代码的万金油：它们是异步函数用来传递结果给callbacks的信号，但是使用Deferred不保证你得到一个异步函数
"""


"""
优化重写，保证异步
关键点：twisted.internet.threads.deferToThread
"""

# from twisted.internet import threads, reactor
# import time
#
# def largeFibonnaciNumber():
#     t = 1000000
#
#     first = 0
#     second = 1
#
#     for i in range(t-1):
#         new = first + second
#         first = second
#         second = new
#
#         if i%10000 == 0:
#             print(f"calculating the {i}th Fibonnaci number")
#
#     return second
#
#
# def fiboCallback(result):
#     print(f"large Fibo result is ***")
#     reactor.stop()
#
#
# def run():
#     start_time = time.time()
#     d = threads.deferToThread(largeFibonnaciNumber)
#     d.addCallback(fiboCallback)
#     end_time = time.time() - start_time
#     print(f"spend time {end_time}")
#
#
# if __name__ == "__main__":
#     run()
#     reactor.run()


"""
Reactor主要有如下两个功能：

监视一系列与你I/O操作相关的文件描述符（description)。监视文件描述符的过程是异步的，也就是说整个循环体是非阻塞的；
不停地向你汇报那些准备好的I/O操作的文件描述符。

在一个回调函数执行过程中，实际上Twisted的循环是被有效地阻塞在我们的代码上的。因此，我们应该确保回调函数不要浪费时间（尽快返回）。特别需要强调的是，我们应该尽量避免在回调函数中使用会阻塞I/O的函数。否则，我们将失去所有使用reactor所带来的优势。Twisted是不会采取特殊的预防措施来防止我们使用可阻塞的代码的，这需要我们自己来确保上面的情况不会发生。正如我们实际看到的一样，对于普通网络I/O的例子，由于我们让Twisted替我们完成了异步通信，因此我们无需担心上面的事情发生。

Transport：一个Twisted的Transport代表一个可以收发字节的单条连接
Protocol：Twisted的Protocol抽象由interfaces模块中的IProtocol定义。Protocol定义了数据传输的协议，如TCP、FTP、IMAP或其他我们自己制定的协议
Protocol Factory：一个连接对应一个Protocol实例，Protocol Factory会为每一个连接创建一个Protocol实例；Protocol Factory的API由IProtocolFactory来定义，同样在interfaces模块中。Protocol Factory就是Factory模式的一个具体实现。buildProtocol方法在每次被调用时返回一个新Protocol实例，它就是Twisted用来为新连接创建新Protocol实例的方法；
"""