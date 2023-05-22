import discord
from discord import app_commands
import asyncio
import datetime
from crawler.FBGroupCrawler import FBGroupCrawler
import typing
import functools
import os
from dotenv import load_dotenv

load_dotenv()

RENT_CHANNEL_URL = os.getenv('RENT_CHANNEL_URL')
RENT_CHANNEL = int(''.join(RENT_CHANNEL_URL.split()).split('/')[-1])
FB_GROUP_IDS = {
    '520481528006179': '海洋大學租屋網',
    '1630046830621490': '基隆店面、套房、公寓租屋',
    '914015935379410': '基隆租屋.求租.交流園地',
    # '718510849156773': '海大基隆租屋資訊網', # error, don't know why

    # 不太優
    '486851054845946': '海大租屋 (只有一個房東)',
    '449394203135088': '基隆租屋時代',

    # 較多非相關資訊 (如售屋資訊)
    # '2428433154057562': '基隆租屋、基隆求租、基隆出租、汐止租屋、七堵租屋、專屬社團3.0',
    '924551210951098': '基隆租屋、基隆求租、基隆出租、汐止租屋、七堵租屋、專屬社團',
    '1610833912348270': '基隆租屋網～房東留言po文',
    '203051300060991': '基隆區*租屋*售屋*賣屋*', # strange error
    '181198918716569': '基隆房屋租售服務平台',
    '489490087748027': '基隆（買屋賣屋租屋最新資訊）', # strange error
    '858092290933021': '基隆人買賣房屋租屋討論區',
    '495611707675228': '基隆房屋買賣租賃平台',
    '1089988484774919': '基隆買屋 賣屋 租屋',
    '1490093761253669': '基隆買屋/售屋/租屋/危險及老舊建築物重建諮詢', # strange error
    '1938590273028376': '基隆房屋買賣租售大平台',
    '877782089280551': '基隆,賣屋,買屋,租屋,店面',


    # 除了租屋資訊外，還有其他資訊 (如求職資訊)
    '1571604756461696': '基隆 租屋 售屋 求職 專區',
    # '584947098363857': '基隆租屋.售屋.售地.求租.求售.交流園地',
    # '136373057089594': '基隆租售房屋網',
    
    # 待處理
    # '1909931595999769': '基隆、瑞芳、汐止、金山、萬里，🏠🏠🏠售屋，租屋，免費鑑價，代管中心🏠🏠🏠',
    # '1429626127364155': '瑞芳、基隆、租屋賣房停車位家具出清、二手物品分享',
    # 'bagc2004': '基隆好康網(基隆租屋-售屋-求職-求才-美食資訊)',
    # '795613610976991': '大台北房東自租租屋網 (私密社團)',
    # 'renthousetaipeicity': '租屋售屋平台●大台北地區',
    # '492557384206423': 'NTOU 租屋看房去 (私密社團)',
}

IGNORE_KEYWORDS = [
    '求租', '找租', '預算', '自用', # 求租
    '限男生', '社會住宅', # 條件不符
    '開價', '售價', '出價', '總價', '出售', '成交', '交屋', '買屋', '售屋', '不動產', '有巢氏', '\d\d\d萬', # 售屋
    '索票', '搬家', '車位出租', '貨到付款', '清潔', '未拆封', '誠徵', # 其他
]
KITCHEN_KEYWORDS = ['炊', '伙', '煮', '廚', '電磁爐', '瓦斯爐']
STUDIO_KEYWORDS = ['套房']
ROOMING_HOUSE_KEYWORDS = ['雅房']
OUTSIDE_FACING_WINDOWS_KEYWORDS = ['外窗']


def filter_posts(
    posts,
    is_kitchen_required=False,
    is_studio_required=False,
    is_rooming_house_required=False,
    is_outside_facing_windows_required=False,
    end_date=None,
):
    filtered_posts = posts[~posts['CONTENT'].str.contains(
        '|'.join(IGNORE_KEYWORDS),
        regex=True,
        case=False
    )]
    if is_kitchen_required:
        filtered_posts = filtered_posts[filtered_posts['CONTENT'].str.contains(
            '|'.join(KITCHEN_KEYWORDS),
            regex=True,
            case=False
        )]
    if is_studio_required:
        filtered_posts = filtered_posts[filtered_posts['CONTENT'].str.contains(
            '|'.join(STUDIO_KEYWORDS),
            regex=True,
            case=False
        )]
    if is_rooming_house_required:
        filtered_posts = filtered_posts[filtered_posts['CONTENT'].str.contains(
            '|'.join(ROOMING_HOUSE_KEYWORDS),
            regex=True,
            case=False
        )]
    if is_outside_facing_windows_required:
        filtered_posts = filtered_posts[filtered_posts['CONTENT'].str.contains(
            '|'.join(OUTSIDE_FACING_WINDOWS_KEYWORDS),
            regex=True,
            case=False
        )]
    if end_date is not None:
        filtered_posts = filtered_posts[filtered_posts['TIME'] <= end_date]
    # filter empty posts
    filtered_posts = filtered_posts[filtered_posts['CONTENT'].str.len() > 0]
    return filtered_posts


def format_post_message(post, group_id, group_name):
    message_text = '=' * 20 + '\n'
    message_text += f'**URL**: https://www.facebook.com/groups/{group_id}/permalink/{post["POSTID"]}\n'
    message_text += f'**GROUP**: {group_name}\n'
    message_text += f'**TIME**: {post["TIME"]}\n'
    message_text += f'**AUTHOR**: {post["NAME"]} ({post["ACTORID"]})\n'
    # message_text += f'**COMMENT/LIKE/SHARE**: {post["COMMENTCOUNT"]}/{post["LIKECOUNT"]}/{post["SHARECOUNT"]}\n'
    if post['CONTENT'] != '':
        message_text += f'**CONTENT**: \n{post["CONTENT"]}\n'
    return message_text


async def add_reaction_by_keywords(message, content, keywords, reaction):
    if any(keyword in content for keyword in keywords):
        await message.add_reaction(reaction)

async def add_reactions(message, content):
    await add_reaction_by_keywords(message, content, KITCHEN_KEYWORDS, '🔥')
    await add_reaction_by_keywords(message, content, STUDIO_KEYWORDS, '🛁')
    await add_reaction_by_keywords(message, content, ROOMING_HOUSE_KEYWORDS, '🛏️')
    await add_reaction_by_keywords(message, content, OUTSIDE_FACING_WINDOWS_KEYWORDS, '🌞')

class MyClient(discord.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self) -> None:
        # create the background task and run it in the background
        self.bg_task = self.loop.create_task(self.my_background_task())

    async def on_ready(self):
        print(f'Logged in as {self.user} (ID: {self.user.id})')
        synced = await self.tree.sync()
        print(f'Synced {len(synced)} commands')
        print('------')


    async def run_blocking(self, blocking_func: typing.Callable, *args, **kwargs) -> typing.Any:
        """Runs a blocking function in a non-blocking way"""
        func = functools.partial(blocking_func, *args, **kwargs) # `run_in_executor` doesn't support kwargs, `functools.partial` does
        return await self.loop.run_in_executor(None, func)

    async def my_background_task(self):
        await self.wait_until_ready()
        self.last_post_id_dict = {group_id: None for group_id in FB_GROUP_IDS}
        self.last_post_time_dict = {group_id: None for group_id in FB_GROUP_IDS}
        self.start_time = datetime.datetime.now()
        rent_channel = self.get_channel(RENT_CHANNEL)

        start_time_str = self.start_time.strftime('%Y-%m-%d %H:%M:%S')
        await rent_channel.send(f'Start fetching latest posts on {start_time_str}...')

        while not self.is_closed():
            current_time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f'====== Start fetching latest posts on {current_time_str}...')
            for group_id, group_name in FB_GROUP_IDS.items():
                print(f'Fetching group: {group_name}({group_id})...')
                crawler = FBGroupCrawler(group_id)
                posts = None
                if self.last_post_id_dict[group_id] == None:
                    start_date_str = self.start_time.strftime('%Y-%m-%d')
                    print(f'Fetching posts after date: {start_date_str}...')
                    # posts = crawler.crawl_posts_after_date(start_date_str)
                    posts = await self.run_blocking(crawler.crawl_posts_after_date, start_date_str)
                    if len(posts) > 0:
                        self.last_post_id_dict[group_id] = posts['POSTID'].max()
                        self.last_post_time_dict[group_id] = posts['TIME'].max()
                else:
                    print(f'Fetching posts after id: {self.last_post_id_dict[group_id]} ({self.last_post_time_dict[group_id]})...')
                    # posts = crawler.crawl_posts_after_post(
                    #     self.last_post_id_dict[group_id], self.last_post_time_dict[group_id])
                    posts = await self.run_blocking(crawler.crawl_posts_after_post, self.last_post_id_dict[group_id], self.last_post_time_dict[group_id])
                    if len(posts) > 0:
                        self.last_post_id_dict[group_id] = posts['POSTID'].max()
                        self.last_post_time_dict[group_id] = posts['TIME'].max()

                posts = filter_posts(posts)

                if len(posts) > 0:
                    for idx, post in posts.iterrows():
                        message_text = format_post_message(post, group_id, group_name)
                        message = await rent_channel.send(message_text)
                        await add_reactions(message, post['CONTENT'])
                print(f'Finished fetching group: {group_name}({group_id})...')
                await asyncio.sleep(10)
            
            print('====== Finished fetching latest posts.')
            await asyncio.sleep(900)
    
def run(token):
    client = MyClient(intents=discord.Intents.default(), command_prefix='!')

    @client.tree.command(name='test')
    async def test(interation: discord.Interaction):
        await interation.response.send_message('test')
    
    @client.tree.command(name='show_all_groups')
    async def show_all_groups(interation: discord.Interaction):
        await interation.response.defer()
        message_text = ""
        for group_id, group_name in FB_GROUP_IDS.items():
            message_text += f'{group_name}({group_id})\n'
        await interation.followup.send(message_text)

    @client.tree.command(name='fetch_posts')
    @app_commands.describe(
        start_date='Start date to fetch posts (YYYY-MM-DD)',
        end_date='End date to fetch posts (YYYY-MM-DD)',
        group_id='Group id to fetch posts from',
        is_kitchen_required='filter kitchen(🔥) posts',
        is_studio_required='filter studio(🛁) posts',
        is_windows_required='filter outside facing windows(🌞) posts',
    )
    async def fetch_posts(
        interation: discord.Interaction,
        group_id: str,
        start_date: str,
        end_date: str = None,
        is_kitchen_required: bool = False,
        is_studio_required: bool = False,
        is_windows_required: bool = False,
    ):
        await interation.response.defer()
        group_name = FB_GROUP_IDS[group_id]
        await interation.followup.send(
            f'Fetching posts from {group_name}({group_id}) between {start_date} and {end_date}...'
        )
        crawler = FBGroupCrawler(group_id)
        posts = await client.run_blocking(crawler.crawl_posts_after_date, start_date)
        posts = filter_posts(
            posts,
            is_kitchen_required=is_kitchen_required,
            is_studio_required=is_studio_required,
            is_outside_facing_windows_required=is_windows_required,
            end_date=end_date,
        )
        for idx, post in posts.iterrows():
            message_text = format_post_message(post, group_id, group_name)
            message = await interation.followup.send(message_text)
            await add_reactions(message, post['CONTENT'])
            await asyncio.sleep(1)
        await interation.followup.send(f'Finished fetching posts. Found {len(posts)} posts.')

    client.run(token)