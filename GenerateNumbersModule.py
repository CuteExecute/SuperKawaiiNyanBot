import random

list_of_nums = []
users_list = []


def get_rnd_nums_for_users(nums_of_img: int, img_per_user: int, users: int) -> list:
    list_of_nums = list(range(1, nums_of_img + 1))
    for i in range(0, int(users)):
        temp_list = []
        for _el in range(0, img_per_user):
            temp_rnd_num = list_of_nums[random.randint(0, len(list_of_nums) - 1)]
            temp_list.append(temp_rnd_num)
            list_of_nums.remove(temp_rnd_num)
        users_list.append(temp_list)
    return users_list


def clear_lists():
    list_of_nums.clear()
    users_list.clear()
