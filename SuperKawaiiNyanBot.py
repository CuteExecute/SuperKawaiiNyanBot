import discord
from discord.ext import commands
from GenerateNumbersModule import get_rnd_nums_for_users, clear_lists
import DbHelper
import datetime

EVENT_TIME = {
    'morning': 'mrg',
    'day': 'day',
    'evening': 'evg'
}

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)


def token_read() -> str:
    file = open("key.txt", "r")
    lines = file.readlines()
    return lines[0]


TOKEN = f'{token_read()}'


def create_show_string(array) -> str:
    final_str = ''
    for el in array:
        final_str += f'{el}\n'
    return final_str


def message_get_params_by_split(message) -> []:
    parts_01 = message.split('|')
    parts_02 = parts_01[1]
    parts_03 = parts_02.split(' ')
    parts_03.remove('**')
    return parts_03


def print_scoreboard_month() -> str:
    scoreboard_string = ''
    scoreboard = DbHelper.scoreboard_month()

    counter = 1
    for key in scoreboard:
        scoreboard_string += f'\n{counter}| {scoreboard[key]} - {DbHelper.get_username_by_discord_id(key)}'
        counter += 1

    return f'```ansi\n{scoreboard_string}```'


def print_profile_pretty(ctx) -> str:
    event_code = {
        'morning': 'утро',
        'day': 'день',
        'evening': 'вечер'
    }

    marks_list = DbHelper.select_all_marks_count_per_month_for_user(ctx.author.id)
    select_marks = DbHelper.select_marks_by_user_per_month(ctx.author.id)

    profile_string = f'{ctx.author}'
    profile_string += f'\n__________________________'
    profile_string += f'\nобщее количество : {marks_list[3]}'
    profile_string += f'\nутро             : {marks_list[0]}'
    profile_string += f'\nдень             : {marks_list[1]}'
    profile_string += f'\nвечер            : {marks_list[2]}'
    profile_string += f'\n__________________________'

    # 30: gray; 31: red; 32: py green; 33: py orange; 34: blue; 35: magnetta; 36: cyan; 37: white default; - ui palette
    text_color_good = '36'
    text_color_bad = '31'

    for row in select_marks:
        profile_string += f'\n[0;0m{row[1]}'
        if row[2] == True:
            profile_string += f" [0;{text_color_good}m{event_code['morning']}"
        else:
            profile_string += f" [0;{text_color_bad}m{event_code['morning']}"
        if row[3] == True:
            profile_string += f" [0;{text_color_good}m{event_code['day']}"
        else:
            profile_string += f" [0;{text_color_bad}m{event_code['day']}"
        if row[4] == True:
            profile_string += f" [0;{text_color_good}m{event_code['evening']}"
        else:
            profile_string += f" [0;{text_color_bad}m{event_code['evening']}"

    return f'```ansi\n{profile_string}```'


@bot.event
async def on_ready():
    print('Started!')


@bot.event
async def on_raw_reaction_add(payload):
    if payload.emoji.name == '✅':
        user = await bot.fetch_user(payload.user_id)
        username = user.name
        message = await bot.get_channel(payload.channel_id).fetch_message(payload.message_id)

        date_and_event_time = message_get_params_by_split(message.content)
        message_data = {
            'event_time': date_and_event_time[0],
            'date': date_and_event_time[1]
        }

        DbHelper.mark_set_for_user_by_date(username, payload.user_id, message_data['event_time'], message_data['date'])
        print(f"[on_raw_reaction_add] "
              f"- {username} "
              f"- {payload.user_id} "
              f"- {message_data['event_time']} "
              f"- {message_data['date']}")


@bot.event
async def on_raw_reaction_remove(payload):
    if payload.emoji.name == '✅':
        message = await bot.get_channel(payload.channel_id).fetch_message(payload.message_id)

        date_and_event_time = message_get_params_by_split(message.content)
        message_data = {
            'event_time': date_and_event_time[0],
            'date': date_and_event_time[1]
        }

        DbHelper.mark_remove_for_user_by_date(payload.user_id, message_data['event_time'], message_data['date'])
        print(f"[on_raw_reaction_remove] - {payload.user_id} - {message_data['event_time']} - {message_data['date']}")


@bot.command(name='m', aliases=['mrg', 'morning'])
async def mrg(ctx):
    await ctx.send(f"УТРО - для отметки ставь реакцию ✅\n**|** mrg {datetime.datetime.now().date()}")


@bot.command(name='d', aliases=['day'])
async def day(ctx):
    await ctx.send(f"ДЕНЬ - для отметки ставь реакцию ✅\n**|** day {datetime.datetime.now().date()}")


@bot.command(name='e', aliases=['evg', 'evening'])
async def evg(ctx):
    await ctx.send(f"ВЕЧЕР - для отметки ставь реакцию ✅\n**|** evg {datetime.datetime.now().date()}")


@bot.command(name='lc', aliases=['local'])
async def lc(ctx):
    if not DbHelper.is_user_exists(ctx.author.id):
        user_create(name, dis_id)
    else:
        print('THIS USER IS ALREADY EXIST')
    DbHelper.delete_empty_string_for_user(ctx.author.id)
    await ctx.send(print_profile_pretty(ctx))


@bot.command(name='sb', aliases=['scoreboard'])
async def scoreboard(ctx):
    await ctx.send(print_scoreboard_month())


@bot.command(name='shw', aliases=['show'])
async def show(ctx):
    if ctx.author.id == 148771435517706240:
        if str(create_show_string(DbHelper.show())):
            await ctx.send(f'```{create_show_string(DbHelper.show())}```')
        else:
            await ctx.send(f'```NO DATA```')
    else:
        await ctx.send(f'Сер, {ctx.author.mention} - вы ошиблись! :clown:')


@bot.command(name='dbclear', aliases=['fucked_up_beyond_all_recognition'])
async def fucked_up_beyond_all_recognition(ctx):
    if ctx.author.id == 148771435517706240:
        DbHelper.clear_all_table()
        await ctx.send('Clear all results in tables complete!')
    else:
        await ctx.send(f'Сер, {ctx.author.mention} - вы ошиблись! :clown:')


@bot.command(name='n', aliases=['nums'])
async def nums(ctx, nums_of_img, img_per_user, users):
    try:
        if checked(int(nums_of_img), int(img_per_user), int(users)):
            results = get_rnd_nums_for_users(int(nums_of_img), int(img_per_user), int(users))
            str = 'Леонид Якубович, вращайте барабан!\n'
            for n in results:
                str += f'{n}\n'
            await ctx.send(f'```{str}```')
            clear_lists()
            results.clear()
        else:
            await ctx.send(f'Сер, {ctx.author.mention} - вы ошиблись! :clown:')
    except:
        await ctx.send(f'Сер, {ctx.author.mention} - вы ошиблись! :clown:')


@bot.command(name='ndbg', aliases=['numsdbg'])
async def numsdbg(ctx, nums_of_img, img_per_user, users):
    try:
        if checked(int(nums_of_img), int(img_per_user), int(users)):
            results = get_rnd_nums_for_users(int(nums_of_img), int(img_per_user), int(users))
            percent_unused_imgs = (int(nums_of_img) - (int(users) * int(img_per_user))) * 100 / int(nums_of_img)
            str = f'незадействованных картинок: {percent_unused_imgs}%\n'
            str += 'результаты:\n'
            count = 1
            for n in results:
                if count % 5 == 0:
                    str += f'[0;31m{n} - {count}\n'  # 31 red 34 blue 36 cyan
                else:
                    str += f'[0;0m{n}\n'
                count += 1
            await ctx.send(f'```ansi\n{str}```')
            clear_lists()
            results.clear()
        else:
            await ctx.send(f'Сер, {ctx.author.mention} - вы ошиблись! :clown:')
    except:
        await ctx.send(f'Сер, {ctx.author.mention} - вы ошиблись! :clown:')


def checked(nums_of_img: int, img_per_user: int, users: int):
    if (users * img_per_user) <= nums_of_img \
            and (nums_of_img <= 500 and img_per_user <= 500 and users <= 500):
        return True
    else:
        return False


bot.run(f'{TOKEN}')
