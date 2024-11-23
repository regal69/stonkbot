import asyncio
import discord
import math
import os
import random
import re
import sqlite3
from collections import Counter, namedtuple
from datetime import datetime, timedelta
from discord.ext import commands, tasks

######### Definitions #########

# namedtuple that holds data about occurences over a given period in a single channel.
#   msg_count: int, number of messages
#   emoji_counts: Counter{str emoji name, int count}
ChannelCountResult = namedtuple('ChannelCountResult', ['msg_count', 'emoji_counts'])

# same as above, but channel_counts is a Counter
ServerCountResult = namedtuple('ServerCountResult', ['channel_counts', 'emoji_counts'])

######### Bot class #########

class StonkBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True  # Enable message content intent
        super().__init__(command_prefix="$", intents=intents)

        self.conn = sqlite3.connect("stock_market.db")
        self.c = self.conn.cursor()
        self.create_db()

        # vars
        self.emoji_set = None # Set in on_ready() -> initialize_stocks()
        self.ticker_to_name = dict()
        self.name_to_ticker = dict()

    ########### Initialization ###########

    async def on_ready(self):
        print(f"Logged in as {self.user}")
        await self.add_cog(StonkCog(self, self.conn))
        await self.initialize_stocks() 
        self.update_stocks_task.start()

    # Create tables for users and stocks
    def create_db(self):
        # Users table, stores basic user info
        self.c.execute('''CREATE TABLE IF NOT EXISTS users (
                        user_id INTEGER PRIMARY KEY,
                        gamertag TEXT,
                        balance REAL)''')
        # Stores user stocks
        self.c.execute('''CREATE TABLE IF NOT EXISTS stock_holdings (
                        user_id INTEGER,
                        stock_name TEXT,
                        quantity INTEGER,
                        PRIMARY KEY (user_id, stock_name),
                        FOREIGN KEY (user_id) REFERENCES users(user_id))''')
        # Stores stock value information
        self.c.execute('''CREATE TABLE IF NOT EXISTS stocks (
                    stock_name TEXT PRIMARY KEY,
                    stock_value REAL,
                    stock_avail INTEGER)''')
        # Stores metadata such as last update time
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS metadata (
                guild_id INTEGER PRIMARY KEY,
                update_time_iso TEXT)''')
        
        self.conn.commit()

    # Initialize stocks based on message activity in the last 5 days
    async def initialize_stocks(self):
        await self.wait_until_ready()

        self.emoji_set = self.get_emoji_set()

        self.c.execute("SELECT stock_name FROM stocks")
        stocks = {stock_name for (stock_name,) in self.c.fetchall()}

        server_count_result = await self.count_all_occurences_in_server(hours=120) # 5 days

        # Initialize channel stocks
        for channel in self.guilds[0].text_channels:
            ticker_name = self.create_ticker_name("C", channel.name)

            # These maps are stored in memory, so we need to regenerate on startup
            self.ticker_to_name[ticker_name] = channel.name
            self.name_to_ticker[channel.name] = ticker_name
            if ticker_name in stocks:
                continue
            try:
                activity_count = server_count_result.channel_counts[channel.name]
                price = self.get_initial_stock_value(activity_count)

                self.c.execute("INSERT OR IGNORE INTO stocks (stock_name, stock_value, stock_avail) VALUES (?, ?, ?)", 
                        (ticker_name, price, 10000))
                print(f"Created stock {ticker_name} referencing {channel.name} with value {price}")
            except discord.Forbidden:
                # Skip channels that the bot can't access
                print(f"Skipping channel {channel.name} due to insufficient permissions.")
            except Exception as e:
                print(f"An error occurred with channel {channel.name}: {e}")

        
        # Initialize emoji stocks. We need to use a sorted list so the naming is the same across bot startups
        sorted_emoji_list = sorted(list(self.emoji_set))
        for emoji in sorted_emoji_list:
            count = server_count_result.emoji_counts[emoji]
            ticker_name = self.create_ticker_name("E", self.get_emoji_name(emoji))

            # These maps are stored in memory, so we need to regenerate on startup
            self.ticker_to_name[ticker_name] = emoji
            self.name_to_ticker[emoji] = ticker_name
            if ticker_name in stocks:
                continue
            price = self.get_initial_stock_value(count)

            # Insert or update the stock in the database
            self.c.execute("INSERT OR IGNORE INTO stocks (stock_name, stock_value, stock_avail) VALUES (?, ?, ?)", 
                    (ticker_name, price, 10000))
            print(f"Created stock {ticker_name} referencing {emoji} with value {price}")

        self.conn.commit()
        print("Stock initialization complete")

    ############## Helper functions #################

    def get_emoji_set(self):
        # Set comp: emojis have form <a:name:id> if animated, else <:name:id>
        return {
            f"<{'a' if emoji.animated else ''}:{emoji.name}:{emoji.id}>"
            for emoji in self.guilds[0].emojis
        }

    def get_channel_stock_name(self, name_string):
        return f"channel_{name_string[:22]}"

    # get emoji name from string of form <a?:emoji_name:emoji_id>
    def get_emoji_name(self, full_emoji_str):
        return f"{full_emoji_str.split(':')[1]}"

    # Reduce each stock's value by 1%, with jitter
    def decay_all_stock_prices(self):
        self.c.execute("SELECT stock_name FROM stocks")
        stocks = self.c.fetchall()
        for (stock_name,) in stocks:
            self.decay_stock_price(stock_name)
        self.conn.commit()

    # Does not commit
    def decay_stock_price(self, stock_name):
        self.c.execute("SELECT stock_name, stock_value FROM stocks WHERE stock_name = ?", (stock_name,))
        result = self.c.fetchone()
        if not result:
            print(f"Tried to fetch stock {stock_name} (decay), but doesn't exist")
            return
        new_value = self.get_stock_decay_value(result[1])
        self.c.execute("UPDATE stocks SET stock_value = ? WHERE stock_name = ?", (new_value, stock_name))

    def get_stock_decay_value(self, old_value):
        return old_value * (1 + random.uniform(-0.025, -0.015))

    async def increase_all_stock_prices(self):
        server_count_result = await self.count_all_occurences_in_server(1)
        for channel_name in server_count_result.channel_counts:
            stock_name = self.name_to_ticker[channel_name]
            activity_count = server_count_result.channel_counts[channel_name]
            self.increase_stock_price(stock_name, activity_count)
        for emoji_str in server_count_result.emoji_counts:
            stock_name = self.name_to_ticker[emoji_str]
            activity_count = server_count_result.emoji_counts[emoji_str]
            self.increase_stock_price(stock_name, activity_count)
        self.conn.commit()

    # Increase stock prices based on number of messages, with adjusted growth rate
    # DOES NOT COMMIT!
    def increase_stock_price(self, stock_name, total_messages):
        self.c.execute("SELECT stock_value FROM stocks WHERE stock_name = ?", (stock_name,))
        result = self.c.fetchone()
        if not result:
            print(f"Tried to fetch stock {stock_name} (increase), but doesn't exist")
            return
        new_price = self.get_increase_stock_value(result[0], total_messages)
        
        self.c.execute("UPDATE stocks SET stock_value = ? WHERE stock_name = ?", (new_price, stock_name))

    def avail_based_price_adjustment_all_stocks(self):
        self.c.execute("SELECT stock_name, stock_avail FROM stocks")
        result = self.c.fetchall()
        if not result:
            print("Failed to fetch any stocks (avail_based_adjust)")
        
        for stock_name, stock_avail in result:
            # Adjust the availability. Uses a normal distribution centered at 0.15 with a std of 0.075
            # The distribution is used to pick a percentage of the "gap" between the current avail and the target
            # avail to fill at any given time.
            # Add a random small amount at the end to prevent getting stuck at target, then rounds to an int.
            gap = 25000 - stock_avail
            r = max(0, random.gauss(0.05, 0.025))
            new_avail = round(stock_avail + (gap * r) + random.uniform(0, 25))
            
            # Update the stock price in the database
            self.c.execute("UPDATE stocks SET stock_avail = ? WHERE stock_name = ?", (new_avail, stock_name))
        
        # Commit the changes to the database
        self.conn.commit()
        

    def get_increase_stock_value(self, current_price, total_messages):
        for _ in range(total_messages):
            if current_price < 100:
                current_price += 1.0
            elif current_price < 200:
                current_price += 0.8
            elif current_price < 300:
                current_price += 0.5
            elif current_price < 400:
                current_price += 0.2
            elif current_price < 500:
                current_price += 0.1
            else:
                current_price += 0.05
        return current_price

    def calculate_net_worth(self, user_id):
        self.c.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        result = self.c.fetchone()
        if not result:
            print(f"calculate_net_worth: failed to fetch balance for user {user_id}")
        net_worth = result[0]

        # Fetch the user's stock holdings
        self.c.execute('SELECT stock_name, quantity FROM stock_holdings WHERE user_id = ?', (user_id,))
        holdings = self.c.fetchall()

        # Calculate the value of the user's stocks
        for stock_name, quantity in holdings:
            self.c.execute('SELECT stock_value FROM stocks WHERE stock_name = ?', (stock_name,))
            stock = self.c.fetchone()
            if stock:
                stock_price = stock[0]
                net_worth += stock_price * quantity
        
        return net_worth

    # Lazy generator based message counter
    # TODO: deprecated?
    async def count_messages(self, channel, hours=1):
        """
        Counts the number of messages in a given channel over the past `days` days.
        """
        after_time = datetime.now() - timedelta(hours=hours)
        message_count = 0

        async for _ in channel.history(limit=None, after=after_time):
            message_count += 1
        
        return message_count

    # Count all occurences 
    async def count_all_occurences_in_server(self, hours: int):
        channels_counter = Counter()
        emoji_counter = Counter()
        after_time = datetime.now() - timedelta(hours=hours)

        for channel in bot.guilds[0].text_channels:
            try:
                channel_count_result = await self.count_all_occurrences_in_channel(channel.history(limit=None, after=after_time))
                channels_counter[channel.name] += channel_count_result.msg_count
                emoji_counter += channel_count_result.emoji_counts
            except discord.Forbidden:
                # Skip channels that the bot can't access
                print(f"Skipping channel {channel.name} due to insufficient permissions.")
            except Exception as e:
                print(f"An error occurred with channel {channel.name}: {e}")

        return ServerCountResult(channels_counter, emoji_counter)


    # Count all occurrences of specified types in a given channel.
    #   msg_count: number of messages sent in the channel
    #   emoji_counts: Counter{emoji str: count}
    # Param: a channel.history object (async iterator of discord.Message).
    # Returns: a ChannelCountResult namedtuple
    async def count_all_occurrences_in_channel(self, ch) -> ChannelCountResult:
        msg_count = 0
        emoji_count = Counter()
        async for message in ch:
            msg_count += 1
            for word in message.content.split():
                if word in self.emoji_set:
                    emoji_count[word] += 1
        return ChannelCountResult(msg_count, emoji_count)
            
    # Return an initial price for a given activity count
    def get_initial_stock_value(self, activity_count):
        price = 4.20
        if activity_count < 5:
            return 0 # if activity is too low, start with 0, which will be filtered out in various usages
        elif activity_count > 120:
            # If more than ~1 message per hour, set a value based on average.
            average_per_hour = round(activity_count/120)

            # Roughly initialize stock price based on average over past 5 days
            for _ in range(120):
                price = self.get_stock_decay_value(price)
                price = self.get_increase_stock_value(price, average_per_hour)
        return price

    def store_update_time(self, update_time):
        # Store the current update time in the metadata table
        update_time_str = update_time.isoformat()  # Convert datetime to ISO 8601 string
        self.c.execute("INSERT OR REPLACE INTO metadata (guild_id, update_time_iso) VALUES (?, ?)", (self.guilds[0].id, update_time_str,))

        self.conn.commit()

    def get_update_time(self, guild_id):
        # Retrieve the last update time for a specific guild
        self.c.execute("SELECT update_time_iso FROM metadata WHERE guild_id = ?", (guild_id,))
        result = self.c.fetchone()
        if result:
            return datetime.fromisoformat(result[0])  # Convert back to datetime object
        else:
            return None
        
    def check_gamertag(self, gamertag):
        if not gamertag:
            return False
        return re.fullmatch(r'[a-z0-9]+', gamertag) and len(gamertag) <= 11

    # removes everything other than a-zA-Z0-9
    def clean_string(self, s):
        return re.sub(r'[^a-zA-Z0-9]', '', s)

    # Attempts to create a unique ticker name for a given name of a stock
    def create_ticker_name(self, prefix: str, fullname: str):
        if not prefix or not fullname or len(prefix) != 1:
            return "ERROR_TICKER_CREATION"
        
        s = self.clean_string(fullname).upper()
        if (len(s)) >= 5:
            # First, try the string without any alterations
            r = self._iterate_possible_ticker_names(prefix, s);
            if r:
                return r

        # If no valid result or length is too short, we append some chars and try to let the conflict resolution logic
        # handle it until some kind of new result is found
        for i in range(26):
            for j in range(26):
                for k in range(26):
                    for l in range(26):
                        for m in range(26):
                            suffix = chr(65+i) + chr(65+j) + chr(65+k) + chr(65+l) + chr(65+m)
                            r = self._iterate_possible_ticker_names(prefix, s + suffix);
                            if r:
                                return r

        # give up, our map has like 5^26 entries or something
        return None
    
    def _iterate_possible_ticker_names(self, prefix, s):
        for i in range(len(s)):
            for j in range(i+1, len(s)):
                for k in range(j+1, len(s)):
                    for l in range(k+1, len(s)):
                        for m in range(l+1, len(s)):
                            r = prefix + s[i] + s[j] + s[k] + s[l] + s[m]
                            if r not in self.ticker_to_name:
                                return r
        return None
            

    ############## Helpers End ###############

    ############## Task #############

    # Background task to update stock prices every 10 seconds
    @tasks.loop(hours=1)
    async def update_stocks_task(self):
        await self.increase_all_stock_prices()
        self.avail_based_price_adjustment_all_stocks()
        self.decay_all_stock_prices()
        self.store_update_time(datetime.now())
        print("Stock prices updated")

    @update_stocks_task.before_loop
    async def waitr(self):
        await self.wait_until_ready()

############# Commands ##############
class StonkCog(commands.Cog):
    def __init__(self, bot, conn):
        self.bot = bot
        self.conn = conn
        self.c = conn.cursor()
        self.buy_lock = asyncio.Lock()

    # Display current stock prices in an embed
    @commands.command(name="stocks")
    async def show_stocks_embed(self, ctx):
        # Create an embed
        embed = discord.Embed(title="Stock Market", color=discord.Color.blue())

        # Calculate the update time messages
        td = datetime.now() - self.bot.get_update_time(ctx.guild.id)
        mins = td / timedelta(minutes=1)
        embed.set_footer(text=f"Last update was {mins:.2f} minutes ago, next update in {(60-mins):.2f} minutes.")

        self.c.execute("SELECT stock_name, stock_value, stock_avail FROM stocks WHERE stock_value > 0 AND stock_name LIKE 'C%' ORDER BY stock_value DESC")
        channel_stocks = self.c.fetchall()
        channel_stock_list = ""
        for stock_name, stock_value, stock_avail in channel_stocks:
            channel_stock_list += f"`{stock_name:<10} ${stock_value:<8.2f} | {stock_avail}`\n"

        embed.add_field(
            name="Channel (`details c[hannel]`)", 
            value=channel_stock_list,
            inline=True
        )

        self.c.execute("SELECT stock_name, stock_value, stock_avail FROM stocks WHERE stock_value > 0 AND stock_name LIKE 'E%' ORDER BY stock_value DESC LIMIT 20")
        emoji_stocks = self.c.fetchall()
        emoji_stock_list = ""
        for stock_name, stock_value, stock_avail in emoji_stocks:
            emoji_stock_list += f"`{stock_name:<10} ${stock_value:<8.2f} | {stock_avail}`\n"

        embed.add_field(
            name="Emoji (`details e[moji]`)", 
            value=emoji_stock_list, 
            inline=True
        )

        await ctx.send(embed=embed)
    
    @commands.command(name="details")
    async def detailed_view(self, ctx, option: str):
        allowed = {"c", "channels", "e", "emoji"}
        option = option.lower()
        if option not in allowed:
            await ctx.send(f"Allowed options are {allowed}")
            return

        embed = discord.Embed(color=discord.Color.og_blurple())

        stock_list = ""
        stock_list_list=[]

        if option == "c" or option == "channels":
            embed.title = "Channels detailed view, top 25"
            self.c.execute("SELECT stock_name, stock_value, stock_avail FROM stocks WHERE stock_name LIKE 'C%' ORDER BY stock_value DESC LIMIT 25")
            stocks = self.c.fetchall()
            for stock_name, stock_value, stock_avail in stocks:
                stock_list += f"`{self.bot.ticker_to_name[stock_name][:20]:<20}: {stock_name:<10} ${stock_value:<8.2f} | {stock_avail}`\n"
                if len(stock_list) > 600:
                    stock_list_list.append(stock_list)
                    stock_list = ""
            else: # thanks python
                stock_list_list.append(stock_list)
            for stock_list in stock_list_list:
                embed.add_field(
                    name="Channel name | Ticker name | Price | # Available to buy",
                    value=stock_list,
                    inline=False
            )
        elif option == "e" or option == "emoji":
            embed.title = "Emojis detailed view, top 25"
            self.c.execute("SELECT stock_name, stock_value, stock_avail FROM stocks WHERE stock_name LIKE 'E%' ORDER BY stock_value DESC LIMIT 25")
            stocks = self.c.fetchall()
            for stock_name, stock_value, stock_avail in stocks:
                stock_list += f"{self.bot.ticker_to_name[stock_name]}`: {stock_name:<10} ${stock_value:<8.2f} | {stock_avail}`\n"
                if len(stock_list) > 600:
                    stock_list_list.append(stock_list)
                    stock_list = ""
            else:
                stock_list_list.append(stock_list)
            for stock_list in stock_list_list:
                embed.add_field(
                    name="Emoji | Ticker name | Price | # Available to buy",
                    value=stock_list,
                    inline=False
                )

        await ctx.send(embed=embed)
            
    @commands.command(name="register")
    async def register(self, ctx, gamertag: str = None):
        # Check if the user is already registered
        self.c.execute('SELECT user_id FROM users WHERE user_id = ?', (ctx.author.id,))
        result = self.c.fetchone()

        if result:
            await ctx.send("You are already registered!")
        elif not gamertag:
            await ctx.send("You must provide a gamertag! Max 11 characters, lowercase and numbers only")
        elif not self.bot.check_gamertag(gamertag):
            await ctx.send("Invalid gamertag! Max 11 characters, lowercase and numbers only")
        else:
            # Register the user with an initial balance
            self.c.execute('INSERT INTO users (user_id, gamertag, balance) VALUES (?, ?, ?)', (ctx.author.id, gamertag, 100000.0))
            self.conn.commit()
            await ctx.send(f"Welcome! You have been registered with an initial balance of $100000.")

    @commands.command(name="buy")
    async def buy_stock(self, ctx, quantity: int, stock_name: str):
        async with self.buy_lock:
            # Check if the user is registered
            self.c.execute('SELECT user_id FROM users WHERE user_id = ?', (ctx.author.id,))
            result = self.c.fetchone()

            if not result:
                await ctx.send("You need to register first! Use the `$register` command to get started.")
                return

            stock_name = stock_name.upper()

            # Check if the stock exists in the database
            self.c.execute("SELECT stock_value, stock_avail FROM stocks WHERE stock_name = ?", (stock_name,))
            result = self.c.fetchone()

            if not result:
                await ctx.send("Invalid stock name.")
                return

            stock_price, stock_avail = result

            # Check quantity validity
            if quantity <= 0:
                await ctx.send("Positive numbers only")
                return
            elif quantity > stock_avail:
                await ctx.send(f"There are only {stock_avail} available to buy")
                return

            # Check price validity
            if stock_price <= 0:
                await ctx.send("No")
                return

            total_cost = stock_price * quantity

            # Check user's balance
            self.c.execute('SELECT balance FROM users WHERE user_id = ?', (ctx.author.id,))
            balance = self.c.fetchone()[0]

            if balance < total_cost:
                await ctx.send(f"You don't have enough money to buy {quantity} shares of {stock_name}. That would cost {total_cost:.2f}, you can buy at most {math.floor(balance/stock_price)}.")
                return

            # Update balance
            new_balance = balance - total_cost
            self.c.execute('UPDATE users SET balance = ? WHERE user_id = ?', (new_balance, ctx.author.id))

            # Update the user's stock holdings
            self.c.execute('SELECT quantity FROM stock_holdings WHERE user_id = ? AND stock_name = ?', (ctx.author.id, stock_name))
            result = self.c.fetchone()

            if result:
                # User already owns this stock, increase the quantity
                new_quantity = result[0] + quantity
                self.c.execute('UPDATE stock_holdings SET quantity = ? WHERE user_id = ? AND stock_name = ?', (new_quantity, ctx.author.id, stock_name))
            else:
                # User doesn't own this stock, add it
                self.c.execute('INSERT INTO stock_holdings (user_id, stock_name, quantity) VALUES (?, ?, ?)', (ctx.author.id, stock_name, quantity))

            # Update stock availability
            new_avail = stock_avail - quantity
            self.c.execute('UPDATE stocks SET stock_avail = ? WHERE stock_name = ?', (new_avail, stock_name))

            self.conn.commit()
            await ctx.send(f"You bought {quantity} shares of {stock_name} for ${total_cost:.2f}. Your new balance is ${new_balance:.2f}. There are {new_avail} left for purchase.")

    @commands.command(name="sell")
    async def sell_stock(self, ctx, quantity: int, stock_name: str):
        # Check if the user is registered
        self.c.execute('SELECT user_id FROM users WHERE user_id = ?', (ctx.author.id,))
        result = self.c.fetchone()

        if not result:
            await ctx.send("You need to register first! Use the `$register` command to get started.")
            return

        stock_name = stock_name.upper()

        # Check if the stock exists in the database
        self.c.execute("SELECT stock_value FROM stocks WHERE stock_name = ?", (stock_name,))
        stock = self.c.fetchone()

        if not stock:
            await ctx.send("Invalid stock name.")
            return

        stock_price = stock[0]
        total_sale = stock_price * quantity

        # Get the user's stock holdings
        self.c.execute('SELECT quantity FROM stock_holdings WHERE user_id = ? AND stock_name = ?', (ctx.author.id, stock_name))
        result = self.c.fetchone()

        if not result or result[0] < quantity:
            await ctx.send(f"You don't own enough shares of {stock_name} to sell.")
            return

        # User has enough stock to sell
        new_quantity = result[0] - quantity
        if new_quantity == 0:
            self.c.execute('DELETE FROM stock_holdings WHERE user_id = ? AND stock_name = ?', (ctx.author.id, stock_name))
        else:
            self.c.execute('UPDATE stock_holdings SET quantity = ? WHERE user_id = ? AND stock_name = ?', (new_quantity, ctx.author.id, stock_name))

        # Update the user's balance
        self.c.execute('SELECT balance FROM users WHERE user_id = ?', (ctx.author.id,))
        balance = self.c.fetchone()[0]
        new_balance = balance + total_sale
        self.c.execute('UPDATE users SET balance = ? WHERE user_id = ?', (new_balance, ctx.author.id))

        # This is atomic, according to chatpit
        self.c.execute("UPDATE stocks SET stock_avail = stock_avail + ? WHERE stock_name = ?", (quantity, stock_name))

        self.conn.commit()
        await ctx.send(f"You sold {quantity} shares of {stock_name} for ${total_sale:.2f}. Your new balance is ${new_balance:.2f}.")

    @commands.command(name="portfolio")
    async def view_portfolio(self, ctx):
        # Retrieve and display the user's portfolio
        self.c.execute('SELECT balance FROM users WHERE user_id = ?', (ctx.author.id,))
        
        result = self.c.fetchone()
        if not result:
            await ctx.send("You need to register first! Use the `$register` command to get started.")
            return

        balance = result[0]

        # Retrieve the user's stock holdings
        self.c.execute('SELECT stock_name, quantity FROM stock_holdings WHERE user_id = ?', (ctx.author.id,))
        holdings = self.c.fetchall()

        portfolio_message = f"**Your Portfolio**\nBalance: ${balance:.2f}\n\nStock Holdings:\n"
        if holdings:
            for stock_name, quantity in holdings:
                portfolio_message += f"{stock_name}: {quantity} shares\n"
        else:
            portfolio_message += "You don't own any stocks."

        await ctx.send(portfolio_message)

    @commands.command(name="leaderboard")
    async def leaderboard(self, ctx):
        # Fetch all user balances
        self.c.execute("SELECT user_id, gamertag FROM users")
        users = self.c.fetchall()

        # Calculate net worth for each user
        leaderboard_data = [(user_id, gamertag, self.bot.calculate_net_worth(user_id)) for (user_id, gamertag) in users]

        # Sort users by net worth in descending order and get the top 15
        leaderboard_data.sort(key=lambda x: x[2], reverse=True)
        top_15 = leaderboard_data[:15]

        # Create an embed for the leaderboard
        embed = discord.Embed(title="Net Worth Leaderboard", color=discord.Color.gold())
        
        # Format leaderboard entries into a single string
        leaderboard_message = ""
        for rank, (user_id, gamertag, net_worth) in enumerate(top_15, start=1):
            user = await self.bot.fetch_user(user_id)
            display_name = user.display_name if user else "Unknown User"
            leaderboard_message += f"`{rank:>2}. {display_name:<15} ({gamertag}) ${net_worth:.2f}`\n"

        # Add the leaderboard as a single field
        embed.add_field(name="Top Users", value=leaderboard_message, inline=False)

        await ctx.send(embed=embed)

    @commands.command(name="networth")
    async def networth_command(self, ctx):
        await ctx.send(f"Your net worth is {self.bot.calculate_net_worth(ctx .author.id):.2f}.")

    @commands.command(name="rename")
    @commands.cooldown(rate=1, per=86400, type=commands.BucketType.user) # 1 day
    async def rename_gamertag(self, ctx, gamertag: str = None):
        if not gamertag:
            await ctx.send("You must provide a gamertag! Max 11 characters, lowercase and numbers only")
            ctx.command.reset_cooldown(ctx)
            return
        elif not self.bot.check_gamertag(gamertag):
            await ctx.send("Invalid gamertag! Max 11 characters, lowercase and numbers only")
            ctx.command.reset_cooldown(ctx)
            return

        # Check if the new gamertag already exists
        self.c.execute("SELECT user_id FROM users WHERE gamertag = ?", (gamertag,))
        if self.c.fetchone():
            await ctx.send(f"The gamertag `{gamertag}` is already taken. Please choose a different one.")
            ctx.command.reset_cooldown(ctx)
            return

        self.c.execute("UPDATE users SET gamertag = ? WHERE user_id = ?", (gamertag, ctx.author.id))
        self.conn.commit()
    
        await ctx.send(f"Your gamertag has been successfully changed to `{gamertag}`.")

    @rename_gamertag.error
    async def rename_error(self, ctx, error):
        if isinstance(error, commands.CommandOnCooldown):
            retry_after = int(error.retry_after)
            hours = retry_after // 3600
            minutes = (retry_after % 3600) // 60
            await ctx.send(f"Max once per day, you can change your gamertag again in {hours}:{minutes}")
    
    @commands.command(name="givemoney")
    async def givemoney(self, ctx, amount: float=0.0, _to: str=None, to_gamertag: str=None):
        if not amount or not _to or not to_gamertag or not isinstance(amount, float):
            await ctx.send("Usage: `give 420.69 to [gamertag]`, use $leaderboard to find gamertag")
            return
        elif amount <= 0:
            await ctx.send("You must specify a positive amount of money.")
            return

        self.c.execute("SELECT user_id FROM users WHERE gamertag = ?", (to_gamertag,))
        target = self.c.fetchone()

        if not target:
            await ctx.send(f"User with gamertag '{to_gamertag}' not found, use $leaderboard to find gamertag")
            return

        giver_id = ctx.author.id
        target_id = target[0]
        self.c.execute("SELECT balance FROM users WHERE user_id = ?", (giver_id,))
        giver_balance = self.c.fetchone()

        if not giver_balance or giver_balance[0] < amount:
            await ctx.send(f"You only have ${giver_balance[0]:.2f}.")
            return

        new_giver_balance = giver_balance[0] - amount
        self.c.execute("UPDATE users SET balance = ? WHERE user_id = ?", (new_giver_balance, giver_id))

        self.c.execute("SELECT balance FROM users WHERE user_id = ?", (target_id,))
        target_balance = self.c.fetchone()

        if not target_balance:
            await ctx.send(f"??? somehow the target has no balance, this should never happen")
            return

        new_target_balance = target_balance[0] + amount
        self.c.execute("UPDATE users SET balance = ? WHERE user_id = ?", (new_target_balance, target_id))

        # Commit the changes
        self.conn.commit()

        # Send confirmation message
        await ctx.send(f"Successfully gave ${amount:.2f} to {to_gamertag}. Your new balance: ${new_giver_balance:.2f}")

    @commands.command(name="givestocks")
    async def givestocks(self, ctx, amount: int=0, stock_name: str=None, _to: str=None, to_gamertag: str=None):
        # Check for invalid or missing inputs
        if not amount or not isinstance(amount, int) or not _to or not to_gamertag or not stock_name:
            await ctx.send("Usage: `givestocks 69 [stock_name] to [gamertag]`, use $leaderboard to find gamertag")
            return
        elif amount <= 0:
            await ctx.send("You must specify a positive amount of stocks.")
            return

        self.c.execute("SELECT user_id FROM users WHERE gamertag = ?", (to_gamertag,))
        target = self.c.fetchone()

        if not target:
            await ctx.send(f"User with gamertag '{to_gamertag}' not found, use $leaderboard to find gamertag")
            return

        giver_id = ctx.author.id
        target_id = target[0]

        self.c.execute("SELECT quantity FROM stock_holdings WHERE user_id = ? AND stock_name = ?", (giver_id, stock_name))
        giver_stock = self.c.fetchone()

        if not giver_stock:
            await ctx.send(f"You don't own {stock_name}.")
            return
        elif giver_stock[0] < amount:
            await ctx.send(f"You only have {giver_stock[0]} {stock_name}.")
            return

        new_giver_stock_balance = giver_stock[0] - amount
        self.c.execute("UPDATE stock_holdings SET quantity = ? WHERE user_id = ? AND stock_name = ?", 
                    (new_giver_stock_balance, giver_id, stock_name))

        self.c.execute("SELECT quantity FROM stock_holdings WHERE user_id = ? AND stock_name = ?", (target_id, stock_name))
        target_stock = self.c.fetchone()

        if not target_stock:
            # If the target has no stocks of this type, insert the new stock record
            self.c.execute("INSERT INTO stock_holdings (user_id, stock_name, quantity) VALUES (?, ?, ?)", 
                        (target_id, stock_name, amount))
        else:
            new_target_stock_balance = target_stock[0] + amount
            self.c.execute("UPDATE stock_holdings SET quantity = ? WHERE user_id = ? AND stock_name = ?", 
                        (new_target_stock_balance, target_id, stock_name))

        self.conn.commit()
        await ctx.send(f"Successfully gave {amount} {stock_name} stocks to {to_gamertag}. Your new stock balance: {new_giver_stock_balance}.")

    # # Debug command to manually advance one hour
    # @commands.command(name="debug_advance")
    # async def advance_hour(self, ctx):
    #     await self.bot.update_stocks_task()
    #     await ctx.send("DEBUG: Advanced time by one hour. Stock prices updated.")

########### Run #############
if __name__ == "__main__":
    bot = StonkBot()
    bot.run(os.getenv("STONK_BOT_TOKEN"))