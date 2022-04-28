import math

test = {
    "n13":2,
    "n12":2,
    "n11":1,
    "n10":1,
    "n9":2,
    "n8":3,
    "n7":3,
    "n6":6,
    "n5":3,
    "n4":8,
    "n3":2,
    "n2":10,
    "n1":4
}

def maiorComum(matchIndex):
    dic = {}
    for value in matchIndex.values():
        if value in dic:
            dic[value] += 1
        else:
            dic[value] = 1

    reversed = sorted(dic.items(),key=lambda x: -x[0])
    sum = 0
    majority = math.floor(len(matchIndex.keys())/2)+1
    for ele in reversed:
        sum += ele[1]
        if sum>=majority:
            return ele[0]


print(maiorComum(test))