# def gen_func():
#     a = yield 4
#     print("a: ", a)
#     b = yield 5
#     print("b: ", b)
#     c = yield 6
#     print("c: ", c)
#     return "finish"
#
#
# if __name__ == '__main__':
#     gen = gen_func()
#     for i in range(4):
#         if i == 0:
#             print(gen.send(None))
#         else:
#             # 因为gen生成器里面只有三个yield，那么只能循环三次。
#             # 第四次循环的时候,生成器会抛出StopIteration异常,并且return语句里面内容放在StopIteration异常里面
#             try:
#                 print(gen.send(i))
#             except StopIteration as e:
#                 print("e: ", e)


def gen_func():
    a = yield 1
    print("a: ", a)
    b = yield 2
    print("b: ", b)
    c = yield 3
    print("c: ", c)
    return 4

def middle():
    gen = gen_func()
    ret = yield from gen
    print("ret: ", ret)
    return "middle Exception"

def main():
    mid = middle()
    for i in range(4):
        if i == 0:
            print(mid.send(None))
        else:
            try:
                print(mid.send(i))
            except StopIteration as e:
                print("e: ", e)


if __name__ == '__main__':
    main()
