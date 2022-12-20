test_list = ["geeksforgeeks is best for geeks"]
chr_list = ["e", "b", "g", "f"]
dict1={}

for i in chr_list:
    dict1[i]=test_list[0].count(i)
        
print(dict1)