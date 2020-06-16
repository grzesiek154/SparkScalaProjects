# squared_list = [x**2 for x in range(5)]



# print(type(squared_list))

# for number in squared_list:
#     print(number)


# squared_gen = (x**2 for x in range(5))


# for number in squared_gen:
#     print(number)

#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# yield
# def cube_numbers(nums):
#     cube_list =[]
#     for i in nums:
#         cube_list.append(i**3)
#     return cube_list

# cubes = cube_numbers([1, 2, 3, 4, 5])

# print(cubes)

def cube_numbers(nums):
    for i in nums:
        yield(i**3)
      

cubes = cube_numbers([1, 2, 3, 4, 5])

print(next(cubes))
print(next(cubes))
print(next(cubes))