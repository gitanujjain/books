def factorial(n):
  if n==0:
    return 1
  if n==1:
    return 1
  if n == 2:
    return 2
  else:
    print(n,end=",")
    return n*factorial(n-1)

for each in range(30):
  print("---",factorial(each))


def two_sum(input: list[int], target: int) -> list[int]:
    for index,num in enumerate(input):
        if target-num in input:
           p2=input.index((target-num))
           if p2!=index:
              return [index,p2 ] 
    else:
        return [-1,-1 ]   