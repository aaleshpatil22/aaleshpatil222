s = "This is a python language"

lst=s.split()
lst1=[]

for i in lst:
    if len(i)%2==0:
        lst1.append(i)

str1=" ".join(lst1)
print(str1)