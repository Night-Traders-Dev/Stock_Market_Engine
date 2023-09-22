from discord.ext import commands, tasks
from discord import Embed, Colour
from tabulate import tabulate
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from discord.utils import get
import decimal
import sqlite3
import random
import discord
import time
import asyncio
import matplotlib.pyplot as plt
import io
import json
import locale
import glob
import shutil
import os
import pickle
import math
import requests
import typing
import re
from math import ceil, floor


# Hardcoded Variables

announcement_channel_ids = [1093540470593962014, 1124784361766650026, 1124414952812326962]
stockMin = 10
stockMax = 150000
dStockLimit = 10000000 #2000000 standard
dETFLimit = 500000
MAX_BALANCE = Decimal('100000000000000000')
sellPressureMin = 0.000011
sellPressureMax = 0.00005
buyPressureMin = 0.000001
buyPressureMax = 0.000045
stockDecayValue = 0.00065 #0.00035 standard
decayMin = 0.01
resetCoins = 100
dailyMin = 10000
dailyMax = 50000
ticketPrice = 100
swap_increments = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, 1000, 10000, 100000, 1000000]
swapThreshold = 50
maxTax = 0.35
last_buy_time = {}
last_sell_time = {}


etf_values = {
    "5 minutes": None,
    "15 minutes": None,
    "30 minutes": None,
    "1 hour": None,
    "3 hours": None,
    "6 hours": None,
    "12 hours": None,
    "24 hours": None,
}

#admins
jacob = 930513222820331590
PBot = 1092870544988319905


#Ledger
ledger_channel = 1150485581739069440
ledger_channel2 = 1150492875474342029
#servers

SludgeSliders = 1087147399371292732
P3 = 1121529633448394973
OM3 = 1070860008868302858
PBL = 1132202921589739540

allowedServers = P3, SludgeSliders, OM3, PBL
##

## WORK
# Define your token earning range
MIN_EARNINGS = 25000
MAX_EARNINGS = 500000



## End Work


##
# Function to split a list into chunks of a specific size
def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


def chunks(data, size):
    it = iter(data)
    for i in range(0, len(data), size):
        yield {k: data[k] for k in list(data)[i:i + size]}

# Define a function to create a page of stocks that can be bought
def create_stock_page(stocks):
    embed = discord.Embed(title="Stocks Available for Purchase", color=discord.Color.blue())
    for stock, amount in stocks.items():
        embed.add_field(name=stock, value=f"Remaining Shares: {amount}", inline=False)
    return embed
##

## Begin Ledger

# Register a custom adapter for decimal.Decimal
sqlite3.register_adapter(Decimal, lambda d: str(d))


# Function to create a connection to the SQLite database
def create_connection():
    return sqlite3.connect("p3ledger.db")

ledger_conn = create_connection()

async def log_transaction(ledger_conn, ctx, action, symbol, quantity, pre_tax_amount, post_tax_amount, balance_before, balance_after, price):
    # Get the user's username from the context
    username = ctx.author.name

    # Convert Decimal values to strings
    pre_tax_amount_str = str(pre_tax_amount)
    post_tax_amount_str = str(post_tax_amount)
    balance_before_str = str(balance_before)
    balance_after_str = str(balance_after)
    price_str = str(price)

    # Create a timestamp for the transaction
    timestamp = ctx.message.created_at.strftime("%Y-%m-%d %H:%M:%S")

    # Insert the transaction into the stock_transactions table
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO stock_transactions (user_id, action, symbol, quantity, pre_tax_amount, post_tax_amount, balance_before, balance_after, price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (ctx.author.id, action, symbol, quantity, pre_tax_amount_str, post_tax_amount_str, balance_before_str, balance_after_str, price_str))
    conn.commit()
    conn.close()

    # Determine whether it's a stock or ETF transaction
    is_etf = True if action in ["Buy ETF", "Sell ETF"] else False

    # Create a timestamp for the transaction
    timestamp = ctx.message.created_at.strftime("%Y-%m-%d %H:%M:%S")

    # Replace CHANNEL_ID with the actual ID of your logging channel
    channel1 = ctx.guild.get_channel(ledger_channel)
    channel2 = ctx.guild.get_channel(ledger_channel2)

    if channel1 and channel2:
        # Create an embed for the log message
        embed = discord.Embed(
            title=f"{username}'s {action} {symbol} {'ETF' if is_etf else 'Stock'} Transaction",
            description=f"Quantity: {quantity}\n"
                        f"Price: {price} coins\n"
                        f"Pre-tax Amount: {pre_tax_amount} coins\n"
                        f"Post-tax Amount: {post_tax_amount} coins\n"
                        f"Balance Before: {balance_before} coins\n"
                        f"Balance After: {balance_after} coins\n"
                        f"Timestamp: {timestamp}",
            color=discord.Color.green() if action.startswith("Buy") else discord.Color.red()
        )

        # Send the log message as an embed to the specified channel
        await channel1.send(embed=embed)
        await channel2.send(embed=embed)




async def log_transfer(ledger_conn, ctx, sender_name, receiver_name, receiver_id, amount):
    # Get the current timestamp in UTC
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Insert the transfer transaction into the transfer_transactions table
    cursor = ledger_conn.cursor()
    cursor.execute("""
        INSERT INTO transfer_transactions (sender_id, receiver_id, amount)
        VALUES (?, ?, ?)
    """, (ctx.author.id, receiver_id, amount))
    ledger_conn.commit()

    # Replace CHANNEL_ID with the actual ID of your logging channel
    channel1 = ctx.guild.get_channel(ledger_channel)
    channel2 = ctx.guild.get_channel(ledger_channel2)

    if channel1 and channel2:
        # Create an embed for the log message with a purple color
        embed = discord.Embed(
            title=f"Transfer from {sender_name} to {receiver_name}",
            description=f"Amount: {amount} coins\n"
                        f"Timestamp (UTC): {timestamp}",
            color=discord.Color.purple()
        )

        # Send the log message as an embed to the specified channel
        await channel1.send(embed=embed)
        await channel2.send(embed=embed)

# Function to log a stock transfer
async def log_stock_transfer(ledger_conn, ctx, sender, receiver, symbol, amount):
    try:
        # Get the current timestamp in UTC
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

#        # Insert the transfer transaction into the transfer_transactions table
#        cursor = ledger_conn.cursor()
#        cursor.execute("""
#            INSERT INTO transfer_transactions (sender_id, receiver_id, symbol, amount, timestamp)
#            VALUES (?, ?, ?, ?, ?)
#        """, (sender.id, receiver.id, symbol, amount, timestamp))
#        ledger_conn.commit()

        # Replace CHANNEL_ID with the actual ID of your logging channel
        channel1 = ctx.guild.get_channel(ledger_channel)
        channel2 = ctx.guild.get_channel(ledger_channel2)

        if channel1 and channel2:
            # Create an embed for the log message with a purple color
            embed = discord.Embed(
                title=f"Stock Transfer from {sender.name} to {receiver.name}",
                description=f"Stock: {symbol}\n"
                            f"Amount: {amount}\n"
                            f"Timestamp (UTC): {timestamp}",
                color=discord.Color.purple()
            )

            # Send the log message as an embed to the specified channels
            await channel1.send(embed=embed)
            await channel2.send(embed=embed)

    except Exception as e:
        # Handle any exceptions that may occur during logging
        print(f"Error while logging stock transfer: {e}")



async def log_burn_stocks(ledger_conn, ctx, stock_name, quantity, price_before, price_after):
    # Get the user's username from the context
    username = ctx.author.name

    # Create a timestamp for the transaction
    timestamp = ctx.message.created_at.strftime("%Y-%m-%d %H:%M:%S")

    # Insert the stock burning transaction into the stock_burning_transactions table
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO stock_burning_transactions (user_id, stock_name, quantity, price_before, price_after)
        VALUES (?, ?, ?, ?, ?)
    """, (ctx.author.id, stock_name, quantity, price_before, price_after))
    conn.commit()
    conn.close()

    # Replace CHANNEL_ID with the actual ID of your logging channel
    channel1 = ctx.guild.get_channel(ledger_channel)
    channel2 = ctx.guild.get_channel(ledger_channel2)

    if channel1 and channel2:
        # Create an embed for the log message
        embed = discord.Embed(
            title=f"{username}'s Stock Burn Transaction",
            description=f"Stock Name: {stock_name}\n"
                        f"Quantity Burned: {quantity}\n"
                        f"Price Before Burn: {price_before} coins\n"
                        f"Price After Burn: {price_after} coins\n"
                        f"Timestamp: {timestamp}",
            color=discord.Color.yellow()
        )

        # Send the log message as an embed to the specified channels
        await channel1.send(embed=embed)
        await channel2.send(embed=embed)

async def log_gambling_transaction(ledger_conn, ctx, game, bet_amount, win_loss, amount_after_tax):
    # Get the user's username from the context
    username = ctx.author.name

    # Create a timestamp for the transaction
    timestamp = ctx.message.created_at.strftime("%Y-%m-%d %H:%M:%S")

    # Insert the gambling transaction into the gambling_transactions table
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO gambling_transactions (user_id, game, bet_amount, win_loss, amount_after_tax)
        VALUES (?, ?, ?, ?, ?)
    """, (ctx.author.id, game, bet_amount, win_loss, amount_after_tax))
    conn.commit()
    conn.close()

    # Replace CHANNEL_ID_1 and CHANNEL_ID_2 with the actual IDs of your ledger channels
    channel1 = ctx.guild.get_channel(ledger_channel)
    channel2 = ctx.guild.get_channel(ledger_channel2)

    if channel1 and channel2:
        # Create an embed for the log message
        embed = discord.Embed(
            title=f"{username}'s {game} Gambling Transaction",
            description=f"Game: {game}\n"
                        f"Bet Amount: {bet_amount} coins\n"
                        f"Win/Loss: {win_loss}\n"
                        f"Amount Paid/Received After Taxes: {amount_after_tax} coins",
            color=discord.Color.orange() if win_loss.startswith("You won") else discord.Color.orange()
        )

        # Send the log message as an embed to both ledger channels
        await channel1.send(embed=embed)
        await channel2.send(embed=embed)
## End Ledger

def setup_ledger():
    try:
        # Create a connection to the SQLite database
        conn = sqlite3.connect("p3ledger.db")
        cursor = conn.cursor()

        # Create a table for stock transactions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_transactions (
                transaction_id INTEGER PRIMARY KEY,
                user_id TEXT NOT NULL,
                action TEXT NOT NULL,
                symbol TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                pre_tax_amount REAL NOT NULL,
                post_tax_amount REAL NOT NULL,
                balance_before REAL NOT NULL,
                balance_after REAL NOT NULL,
                price REAL NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create a table for transfer transactions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transfer_transactions (
                transaction_id INTEGER PRIMARY KEY,
                sender_id TEXT NOT NULL,
                receiver_id TEXT NOT NULL,
                amount REAL NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create a table for stock burning transactions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_burning_transactions (
                transaction_id INTEGER PRIMARY KEY,
                user_id TEXT NOT NULL,
                stock_name TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                price_before REAL NOT NULL,
                price_after REAL NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           )
        """)

        # Create a table for gambling transactions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gambling_transactions (
                transaction_id INTEGER PRIMARY KEY,
                user_id TEXT NOT NULL,
                game TEXT NOT NULL,
                bet_amount REAL NOT NULL,
                win_loss TEXT NOT NULL,
                amount_after_tax REAL NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Create a table for stock transfers
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_transfer_transactions (
                id INTEGER PRIMARY KEY,
                sender_id INTEGER NOT NULL,
                receiver_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                amount INTEGER NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Commit the changes and close the connection
        conn.commit()
        conn.close()
        print("Ledger created successfully")

    except sqlite3.Error as e:
        # Handle the database error
        print(f"Database error: {e}")

    except Exception as e:
        # Handle unexpected errors
        print(f"An unexpected error occurred: {e}")


def setup_database():
    conn = sqlite3.connect("currency_system.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Add available column to stocks table if not exists
    cursor.execute("PRAGMA table_info(stocks)")
    columns = cursor.fetchall()
    if 'available' not in [column[1] for column in columns]:  # Check if 'available' column already exists
        cursor.execute("ALTER TABLE stocks ADD COLUMN available INT")

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                balance BLOB NOT NULL
            )
        """)
    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'users' table: {e}")

    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS raffle_tickets (
                user_id INTEGER PRIMARY KEY,
                quantity INTEGER,
                timestamp INTEGER
            )
        ''')
    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'raffle_tickets' table: {e}")


    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                symbol TEXT PRIMARY KEY,
                available INTEGER64 NOT NULL,
                price REAL NOT NULL,
                coins_required INTEGER NOT NULL,
                coins_rewarded INTEGER NOT NULL
            )
        """)
    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'stocks' table: {e}")

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_stocks (
                user_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                amount REAL NOT NULL,
                FOREIGN KEY (symbol) REFERENCES stocks(symbol),
                PRIMARY KEY (user_id, symbol)
            )
        """)
    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'user_stocks' table: {e}")



    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS etfs (
                etf_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                value REAL NOT NULL DEFAULT 0
             )
         ''')
    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'etfs' table: {e}")

    try:
       cursor.execute('''
           CREATE TABLE IF NOT EXISTS etf_stocks (
                etf_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                FOREIGN KEY (etf_id) REFERENCES etfs(etf_id),
                FOREIGN KEY (symbol) REFERENCES stocks(symbol),
                PRIMARY KEY (etf_id, symbol)
            )
        ''')
    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'etf_stocks' table: {e}")

    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_etfs (
                user_id INTEGER NOT NULL,
                etf_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (etf_id) REFERENCES etfs(etf_id),
                FOREIGN KEY (symbol) REFERENCES etf_stocks(symbol),
                PRIMARY KEY (user_id, etf_id)
            )
        ''')
    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'user_etfs' table: {e}")

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_name TEXT NOT NULL,
                metric_value INTEGER NOT NULL
            )
        """)
    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'metrics' table: {e}")


    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tax_distribution (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                tax_amount DECIMAL(18, 2),
                distribution_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        print("Table 'tax_distribution' created successfully.")
    except sqlite3.Error as e:
        print(f"Error creating table: {e}")


    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS limit_orders (
                order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                order_type TEXT NOT NULL,
                price REAL NOT NULL,
                quantity INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (symbol) REFERENCES stocks(symbol)
            )
        """)
    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'limit_orders' table: {e}")


    try:

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS items (
                item_id INTEGER PRIMARY KEY,
                item_name TEXT NOT NULL,
                item_description TEXT,
                price DECIMAL(18, 2) NOT NULL,
                is_usable INTEGER NOT NULL DEFAULT 0
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS inventory (
                user_id INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (user_id, item_id),
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (item_id) REFERENCES items(item_id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                total_cost DECIMAL(18, 2) NOT NULL,
                tax_amount DECIMAL(18, 2) NOT NULL,
                transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (item_id) REFERENCES items(item_id)
            )
        ''')
        print("Table 'Marketplace' created successfully.")
    except sqlite3.Error as e:
        print(f"Error executing SQL query: {e}")

    try:
        # Create stock_metrics table if not exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_metrics (
                metric_id INTEGER PRIMARY KEY,
                stock_name TEXT NOT NULL,
                date_recorded DATE NOT NULL,
                open_price REAL NOT NULL,
                close_price REAL NOT NULL,
                high_price REAL NOT NULL,
                low_price REAL NOT NULL,
                volume INTEGER NOT NULL
            )
        ''')

    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'stock_metrics' table: {e}")


    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trading_teams (
                team_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                total_profit_loss DECIMAL DEFAULT 0
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS team_members (
                user_id INTEGER,
                team_id INTEGER,
                FOREIGN KEY (team_id) REFERENCES trading_teams(team_id),
                PRIMARY KEY (user_id, team_id)
            );
        """)
        print("Tables created successfully")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the tables: {str(e)}")

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS team_transactions (
                transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                team_id INTEGER,
                symbol TEXT,
                amount INTEGER,
                price DECIMAL,
                type TEXT, -- "buy" or "sell"
                FOREIGN KEY (team_id) REFERENCES trading_teams(team_id)
            );
        """)
        print("Tables created successfully")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the tables: {str(e)}")

    try:
        cursor.execute("""

            CREATE TABLE IF NOT EXISTS user_daily_buys (
                user_id INTEGER,
                symbol TEXT,
                amount INTEGER,
                timestamp DATETIME
            );
        """)
        print("Tables created successfully")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the tables: {str(e)}")


    conn.commit()
    return conn


setup_database()
setup_ledger()



# Begin Firm Engine

def list_teams_leaderboard(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name, total_profit_loss FROM trading_teams
        ORDER BY total_profit_loss DESC
    """)
    leaderboard = cursor.fetchall()
    return leaderboard


def calculate_team_profit_loss(conn, team_id):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT type, amount, price FROM team_transactions WHERE team_id = ?
    """, (team_id,))
    transactions = cursor.fetchall()

    profit_loss = 0
    for type, amount, price in transactions:
        if type == "buy":
            profit_loss -= amount * price
        elif type == "sell":
            profit_loss += amount * price

    cursor.execute("""
        UPDATE trading_teams
        SET total_profit_loss = ?
        WHERE team_id = ?
    """, (profit_loss, team_id))

    conn.commit()
    return profit_loss


def record_team_transaction(conn, team_id, symbol, amount, price, type):
    cursor = conn.cursor()
    price_as_float = float(price)  # Convert the Decimal to a float
    cursor.execute("""
        INSERT INTO team_transactions (team_id, symbol, amount, price, type)
        VALUES (?, ?, ?, ?, ?)
    """, (team_id, symbol, amount, price_as_float, type))  # Use the float value here
    conn.commit()


def join_trading_team(conn, user_id, team_id):
    cursor = conn.cursor()
    cursor.execute("""
        DELETE FROM team_members WHERE user_id = ?
    """, (user_id,))
    cursor.execute("""
        INSERT INTO team_members (user_id, team_id)
        VALUES (?, ?)
    """, (user_id, team_id))
    conn.commit()


def create_trading_team(conn, name):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO trading_teams (name)
        VALUES (?)
    """, (name,))
    conn.commit()


def is_allowed_user(*user_ids):
    async def predicate(ctx):
        return ctx.author.id in user_ids
    return commands.check(predicate)

def is_allowed_server(*server_ids):
    async def predicate(ctx):
        return ctx.guild.id in server_ids
    return commands.check(predicate)


def insert_raffle_tickets(conn, user_id, week, quantity, timestamp):
    cursor = conn.cursor()
    cursor.execute("INSERT INTO raffle_tickets (user_id, week, quantity, timestamp) VALUES (?, ?, ?, ?)",
                   (user_id, week, quantity, timestamp))
    conn.commit()

#stock


class Stock:
    def __init__(self, conn, symbol):
        self.conn = conn
        self.symbol = symbol

    @property
    def name(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM stocks WHERE symbol=?", (self.symbol,))
        result = cursor.fetchone()
        return result["name"] if result else None

    @property
    def price(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT price FROM stocks WHERE symbol=?", (self.symbol,))
        result = cursor.fetchone()
        return result["price"] if result else None

    @property
    def available(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT available FROM stocks WHERE symbol=?", (self.symbol,))
        result = cursor.fetchone()
        return result["available"] if result else None


    @price.setter
    def price(self, value):
        cursor = self.conn.cursor()
        cursor.execute("UPDATE stocks SET price=? WHERE symbol=?", (value, self.symbol))
        self.conn.commit()


class StockMarket:
    def __init__(self, conn):
        self.conn = conn
        self.market_summary.start()

    def get_stock(self, symbol):
        return Stock(self.conn, symbol)


class StockBot(commands.Bot):
    # Define the etf_values dictionary as a class attribute
    etf_values = {
        "5 minutes": None,
        "15 minutes": None,
        "30 minutes": None,
        "1 hour": None,
        "3 hours": None,
        "6 hours": None,
        "12 hours": None,
        "24 hours": None,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def on_ready(self):
        print(f"Logged in as {self.user.name}")

    async def on_command_error(self, ctx, error):
        if isinstance(error, commands.CommandError):
            print(f"Command Error: {error}")
        else:
            raise error



# Economy Engine

def get_global_economy_data(conn):
    cursor = conn.cursor()

    # Fetch data from stocks table
    cursor.execute("SELECT * FROM stocks")
    stocks_data = cursor.fetchall()

    # Fetch data from commodities table
    cursor.execute("SELECT * FROM commodities")
    commodities_data = cursor.fetchall()

    # Fetch data from etfs table
    cursor.execute("SELECT * FROM etfs")
    etfs_data = cursor.fetchall()

    return stocks_data, commodities_data, etfs_data

async def send_stocks_embed(ctx, stocks_data):
    embed = discord.Embed(title="Stocks Information", color=discord.Color.blue())
    for stock in stocks_data:
        embed.add_field(name=stock["symbol"], value=f"Price: {stock['price']}\nAvailable: {stock['available']}", inline=False)
    await ctx.send(embed=embed)


async def send_etfs_embed(ctx, etfs_data):
    embed = discord.Embed(title="ETFs Information", color=discord.Color.gold())
    for etf in etfs_data:
        embed.add_field(name=etf["etf_id"], value=f"Name: {etf['name']}\nValue: {etf['value']}", inline=False)
    await ctx.send(embed=embed)


def update_user_balance(conn, user_id, new_balance):
    if not isinstance(user_id, int) or new_balance is None:
        raise ValueError(f"Invalid user_id or new_balance value. user_id: {user_id}, new_balance: {new_balance}")

    # Ensure that new_balance is a Decimal
    new_balance = Decimal(new_balance)

    # Check if new_balance exceeds the maximum allowed
    if new_balance > MAX_BALANCE:
        raise ValueError(f"User balance exceeds the maximum allowed balance of {MAX_BALANCE} coins.")

    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            balance REAL
        )
    """)

    cursor.execute("""
        INSERT OR REPLACE INTO users (user_id, balance)
        VALUES (?, ?)
    """, (user_id, float(new_balance)))
    conn.commit()

def decay_other_stocks(conn, stock_name_excluded, decay_percentage=stockDecayValue):
    """
    Reduces the value of all stocks other than the one specified by the given percentage.

    :param conn: Database connection
    :param stock_name_excluded: Stock symbol that should be excluded from the decay
    :param decay_percentage: The percentage by which the stocks should be decayed
    """
    cursor = conn.cursor()

    # Get all stocks except the one specified
    cursor.execute("SELECT symbol, price FROM stocks WHERE symbol != ?", (stock_name_excluded,))
    stocks = cursor.fetchall()

    # Apply decay to each stock
    for symbol, price in stocks:
        new_price = Decimal(price) * (1 - Decimal(decay_percentage))
        # Ensure that the new price doesn't go below 0.01
        new_price = max(new_price, Decimal(decayMin))
        cursor.execute("UPDATE stocks SET price = ? WHERE symbol = ?", (str(new_price), symbol))

    conn.commit()



def get_user_balance(conn, user_id):
    cursor = conn.cursor()
    cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()

    if row is None:
        return Decimal(0)

    balance = row[0]

    # If balance is already a float or int
    if isinstance(balance, (float, int)):
        return Decimal(balance)

    # If balance is a serialized string (JSON or otherwise)
    try:
        stripped_balance = balance.strip('"').replace(",", "").rstrip("0").rstrip(".")
        return Decimal(stripped_balance)
    except (ValueError, decimal.InvalidOperation):
        pass

    # If balance is in some other unexpected format, handle it here or raise an error
    raise ValueError("Invalid balance format in the database.")


def get_tax_percentage(quantity: int, cost: Decimal) -> float:
    base_tax = 0.05
    random_factor = random.uniform(0.01, 0.05)  # Randomly fluctuate tax by 1-5%

    # Time-based variability
    current_month = datetime.now().month
    if current_month in [8, 11, 12]:  # For November and December, assume a tax discount for holidays
        seasonal_discount = -0.05
    else:
        seasonal_discount = 0

    # Check if today is Monday
    if datetime.now().weekday() == 0:  # Monday is 0, Tuesday is 1, etc.
        max_tax_rate = 0.10  # Set maximum tax to 10% on Mondays
    else:
        max_tax_rate = maxTax  # Default maximum tax rate (50%)

    # Formulaic approach (based on logarithmic progression)
    quantity_multiplier = 0.01 * (quantity ** 0.5)
    cost_multiplier = 0.01 * (float(cost) / 1000) ** 0.5

    tax_rate = base_tax + quantity_multiplier + cost_multiplier + random_factor + seasonal_discount

    # Limit the tax rate to the maximum tax on Mondays
    tax_rate = min(tax_rate, max_tax_rate)

    # Clipping the tax_rate between 0.01 (1%) to 0.5 (50%) to ensure it's not too low or too high
    tax_rate = max(0.01, min(tax_rate, maxTax))

    return tax_rate


def distribute_tax_amount(conn, tax_percentage, total_cost):
    try:
        cursor = conn.cursor()

        # Calculate the total tax amount
        total_tax = Decimal(total_cost) * Decimal(tax_percentage)

        # Get the total number of users who should pay tax (excluding admins)
        cursor.execute("SELECT COUNT(*) FROM users")
        total_tax_payers = cursor.fetchone()[0]

        if total_tax_payers == 0:
            print("No users found to distribute tax.")
            return

        # Calculate the tax per user
        tax_per_user = total_tax / Decimal(total_tax_payers)

        # Get all user IDs and balances who should pay tax
        cursor.execute("SELECT user_id, balance FROM users")
        user_balances = cursor.fetchall()

        for user_balance in user_balances:
            user_id = user_balance[0]
            balance_value = user_balance[1]

            try:
                # Convert user balance to Decimal format
                current_balance = Decimal(balance_value)
            except InvalidOperation:
                print(f"Invalid balance format for user ID: {user_id}")
                continue

            # Check if the user has enough balance to pay the tax
            if current_balance >= tax_per_user:
                # Update user balance with the new balance after tax deduction
                new_balance = current_balance + tax_per_user
                cursor.execute("UPDATE users SET balance = ? WHERE user_id = ?", (str(new_balance), user_id))

                # Record tax distribution in the "tax_distribution" table
                cursor.execute("INSERT INTO tax_distribution (user_id, tax_amount) VALUES (?, ?)", (user_id, str(tax_per_user)))
            else:
                # If the user does not have enough balance to pay the full tax, distribute the remaining tax amount among other users
                tax_per_user = current_balance
                # Update user balance with the new balance after tax deduction
                new_balance = 0
                cursor.execute("UPDATE users SET balance = ? WHERE user_id = ?", (str(new_balance), user_id))

                # Record tax distribution in the "tax_distribution" table
                cursor.execute("INSERT INTO tax_distribution (user_id, tax_amount) VALUES (?, ?)", (user_id, str(tax_per_user)))

        # Update the metrics table
        cursor.execute("SELECT metric_value FROM metrics WHERE metric_name = 'total_tax_distributed'")
        result = cursor.fetchone()
        current_tax_distributed = Decimal(result[0]) if result else Decimal(0)
        new_tax_distributed = current_tax_distributed + total_tax

        if result:
            cursor.execute("UPDATE metrics SET metric_value = ? WHERE metric_name = 'total_tax_distributed'", (str(new_tax_distributed),))
        else:
            cursor.execute("INSERT INTO metrics (metric_name, metric_value) VALUES (?, ?)", ('total_tax_distributed', str(new_tax_distributed)))

        conn.commit()
        print(f"Total tax distributed: {total_tax} coins")  # Print the total tax distributed amount
    except sqlite3.Error as e:
        print(f"Error executing SQL query: {e}")





def check_and_fill_limit_orders(self, symbol, price, quantity):
    cursor = self.conn.cursor()
    cursor.execute("""
        SELECT order_id, user_id, order_type, price, quantity
        FROM limit_orders
        WHERE symbol=? AND price<=? AND quantity>0
        ORDER BY price ASC, created_at ASC
    """, (symbol, price))

    for order in cursor.fetchall():
        order_id = order["order_id"]
        user_id = order["user_id"]
        order_type = order["order_type"]
        order_price = order["price"]
        order_quantity = order["quantity"]

        if order_type == "buy" and order_price >= price:
            # Fill buy order
            if quantity >= order_quantity:
                self.execute_order(user_id, symbol, "buy", order_quantity, order_price)
                quantity -= order_quantity
            else:
                self.execute_order(user_id, symbol, "buy", quantity, order_price)
                quantity = 0

            # Update remaining quantity in the limit order
            cursor.execute("UPDATE limit_orders SET quantity=? WHERE order_id=?", (order_quantity - quantity, order_id))
            self.conn.commit()

        if quantity == 0:
            break



# End Economy Engine

async def update_market_etf_price(bot, conn):
    guild_id = 1087147399371292732  # Hardcoded guild ID
    channel_id = 1136048044119429160  # Hardcoded channel ID
    guild = bot.get_guild(guild_id)
    channel = get(guild.voice_channels, id=channel_id)

    if channel:
        etf_value = await get_etf_value(conn, 6)  # Replace 6 with the ID of the "Market ETF"
        if etf_value is not None:
            await channel.edit(name=f"Market ETF: {etf_value:,.2f} ")

async def get_etf_value(conn, etf_id):
    cursor = conn.cursor()
    cursor.execute("SELECT symbol, quantity FROM etf_stocks WHERE etf_id=?", (etf_id,))
    stocks = cursor.fetchall()

    # Calculate the value of the ETF
    etf_value = 0
    for stock in stocks:
        symbol = stock[0]
        quantity = stock[1]
        cursor.execute("SELECT price FROM stocks WHERE symbol=?", (symbol,))
        stock_price = cursor.fetchone()
        if stock_price:
            etf_value += stock_price[0] * quantity

    return etf_value



def get_top_ten_users(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            u.user_id,
            SUM(COALESCE(u.balance, 0)) AS wallet_balance,
            SUM(COALESCE(us.amount * s.price, 0)) AS stock_balance,
            SUM(COALESCE(ue.quantity * e.value, 0)) AS etf_balance,
            SUM(COALESCE(u.balance, 0) + COALESCE(us.amount * s.price, 0) + COALESCE(ue.quantity * e.value, 0)) AS total_balance
        FROM
            users AS u
        LEFT JOIN
            user_stocks AS us ON u.user_id = us.user_id
        LEFT JOIN
            stocks AS s ON us.symbol = s.symbol
        LEFT JOIN
            user_etfs AS ue ON u.user_id = ue.user_id
        LEFT JOIN
            etfs AS e ON ue.etf_id = e.etf_id
        GROUP BY
            u.user_id
        ORDER BY
            total_balance DESC
        LIMIT 10
    """)

    top_ten_users = cursor.fetchall()
    return top_ten_users

def get_current_week():
    return (datetime.date.today() - datetime.date(2023, 1, 1)).days // 7

async def get_ticket_data(conn, user_id):
    cursor = conn.cursor()
    cursor.execute("SELECT quantity, timestamp FROM raffle_tickets WHERE user_id = ?", (user_id,))
    return cursor.fetchone()

async def update_ticket_data(conn, user_id, quantity, timestamp):
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO raffle_tickets (user_id, quantity, timestamp) VALUES (?, ?, ?)", (user_id, quantity, timestamp))

async def delete_expired_tickets(conn):
    week = get_current_week()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM raffle_tickets WHERE week < ?", (week,))
    conn.commit()

class CurrencySystem(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.last_claimed = {}
        self.short_targets = {}
        self.players = {}  # Dictionary to store players who join the race
        self.horses = ['🐎', '🐴', '🏇', '🦓', '🦄']  # List of horse emotes
        self.conn = setup_database()
        self.lock = asyncio.Lock()



    def get_available_stocks(self):
        conn = sqlite3.connect("currency_system.db")
        cursor = conn.cursor()
        cursor.execute("SELECT symbol FROM stocks")
        stocks = cursor.fetchall()
        conn.close()
        return [stock[0] for stock in stocks]




#Game Help

    @commands.command(name='game-help', help='Display available commands for Discord Coins.')
    async def game_help(self, ctx):
        stocks_help = """
    View stocks you hold
    ```!my_stocks```

    View current stocks
    ```!stocks```
    ```!list_stocks```

    View specific stock info
    ```!stock_metric P3:BANK```

    Buy stocks, stock name, stock quantity
    ```!buy P3:BANK 100```

    Sell stocks, stock name, stock quantity
    ```!sell P3:BANK 100```
    """

        etfs_help = """
    List ETF's
    ```!list_etfs```

    List your ETF's
    ```!my_etfs```

    Buy ETF
    ```!buy_etf id amount```
    ```!buy_etf 1 1```

    Sell ETF
    ```!sell_etf id amount```
    ```!sell_etf 1 1```

    List ETF Details
    ```!etf_info [etfID]```
    ```!etf_info 1```
    """


        embeds = [
            Embed(title="Stocks Related Commands", description=stocks_help, color=0x0099FF),
            Embed(title="ETF Related Commands", description=etfs_help, color=0x00FF00),
        ]

        current_page = 0
        message = await ctx.send(embed=embeds[current_page])

        # Add reactions for pagination
        reactions = ['⏪', '⏩']
        for reaction in reactions:
            await message.add_reaction(reaction)

        def check(reaction, user):
            return user == ctx.author and reaction.message.id == message.id and str(reaction.emoji) in reactions

        while True:
            try:
                reaction, user = await self.bot.wait_for('reaction_add', timeout=60.0, check=check)
            except TimeoutError:
                break

            if str(reaction.emoji) == '⏪':
                current_page -= 1
            elif str(reaction.emoji) == '⏩':
                current_page += 1

            current_page %= len(embeds)
            await message.edit(embed=embeds[current_page])
            await message.remove_reaction(reaction, user)

        await message.clear_reactions()


    @commands.command(name='tax-bracket', help='Explains the tax bracket system and shows basic examples.')
    async def tax_bracket(self, ctx):
        tax_system = """
    **Tax Bracket System**
    The tax system for stocks is designed to encourage trading activity and balance the economy. The tax percentage is based on the quantity of stocks bought and the cost of the transaction. Here are the tax brackets:

    1. Quantities up to 10:
       - Cost up to 1000: 5% tax
       - Cost above 1000: 10% tax

    2. Quantities between 11 and 50:
       - Cost up to 10000: 15% tax
       - Cost above 10000: 20% tax

    3. Quantities between 51 and 100:
       - Cost up to 50000: 25% tax
       - Cost above 50000: 30% tax

    4. Cost greater than or equal to 100000: 35% tax

    For all other cases, the tax percentage is 10%.
    """

        examples = """
    **Examples**
    1. Buying 5 stocks at a cost of 800 coins:
       - Tax Percentage: 5%
       - Tax Amount: 40 coins

    2. Buying 20 stocks at a cost of 15000 coins:
       - Tax Percentage: 20%
       - Tax Amount: 3000 coins

    3. Buying 70 stocks at a cost of 75000 coins:
       - Tax Percentage: 25%
       - Tax Amount: 18750 coins

    4. Buying 100 stocks at a cost of 110000 coins:
       - Tax Percentage: 30%
       - Tax Amount: 33000 coins

    5. Buying 30 stocks at a cost of 500 coins:
       - Tax Percentage: 10%
       - Tax Amount: 50 coins
    """

        embeds = [Embed(title="Tax Bracket System", description=tax_system, color=0x0099FF),
                  Embed(title="Examples", description=examples, color=0x00FF99)]
        current_page = 0
        message = await ctx.send(embed=embeds[0])

        # Add reactions for pagination
        reactions = ['⏪', '⏩']
        for reaction in reactions:
            await message.add_reaction(reaction)

        def check(reaction, user):
            return user == ctx.author and reaction.message.id == message.id and str(reaction.emoji) in reactions

        while True:
            try:
                reaction, user = await self.bot.wait_for('reaction_add', timeout=60.0, check=check)
            except TimeoutError:
                break

            if str(reaction.emoji) == '⏪':
                current_page -= 1
            elif str(reaction.emoji) == '⏩':
                current_page += 1

            current_page %= len(embeds)
            await message.edit(embed=embeds[current_page])
            await message.remove_reaction(reaction, user)

        await message.clear_reactions()
##



# Bot to Channel


    @commands.command(name='announce', help='Send an announcement to a specific channel.')
    @is_allowed_user(930513222820331590, PBot)
    async def announce(self, ctx, channel_id: int, *, message):
        await ctx.message.delete()
        channel = self.bot.get_channel(channel_id)
        if not channel:
            await ctx.send("Invalid channel ID.")
            return

        await channel.send('@everyone ' + message)
        await ctx.send(f'Announcement sent to {channel.mention}!')

    @commands.command(name='announce-all', help='Send an announcement to specific channels.')
    @is_allowed_user(930513222820331590, PBot)
    async def announce_all(self, ctx, *, message):
        await ctx.message.delete()
        for channel_id in announcement_channel_ids:
            channel = self.bot.get_channel(channel_id)
            if not channel:
                await ctx.send(f"Invalid channel ID: {channel_id}.")
                continue

            await channel.send('@everyone ' + message)
            await ctx.send(f'Announcement sent to {channel.mention}!')

    @commands.command(name='send_bot', help='Send an announcement to a specific channel.')
    @is_allowed_user(930513222820331590, PBot)
    async def send_bot(self, ctx, channel_id: int, *, message):
        await ctx.message.delete()
        channel = self.bot.get_channel(channel_id)
        if not channel:
            await ctx.send("Invalid channel ID.")
            return

        await channel.send(message)


# Metrics

    @commands.command(name="top_wealth", help="Show the top 10 wealthiest users.")
    async def top_wealth(self, ctx):
        cursor = self.conn.cursor()

        await ctx.message.delete()

        # Get the top 10 wealthiest users, sorting them by total wealth (balance + stock value + ETF value)
        cursor.execute("""
            SELECT users.user_id,
                   (users.balance + IFNULL(total_stock_value, 0) + IFNULL(total_etf_value, 0)) AS total_wealth
            FROM users
            LEFT JOIN (
                SELECT user_id, SUM(stocks.price * user_stocks.amount) AS total_stock_value
                FROM user_stocks
                LEFT JOIN stocks ON user_stocks.symbol = stocks.symbol
                GROUP BY user_id
            ) AS user_stock_data ON users.user_id = user_stock_data.user_id
            LEFT JOIN (
                SELECT user_id, SUM(etf_value * user_etfs.quantity) AS total_etf_value
                FROM user_etfs
                LEFT JOIN (
                    SELECT etf_stocks.etf_id, SUM(stocks.price * etf_stocks.quantity) AS etf_value
                    FROM etf_stocks
                    LEFT JOIN stocks ON etf_stocks.symbol = stocks.symbol
                    GROUP BY etf_stocks.etf_id
                ) AS etf_data ON user_etfs.etf_id = etf_data.etf_id
                GROUP BY user_id
            ) AS user_etf_data ON users.user_id = user_etf_data.user_id
            ORDER BY total_wealth DESC
            LIMIT 10
        """)

        top_users = cursor.fetchall()

        if not top_users:
            await ctx.send("No users found.")
            return

        embed = discord.Embed(title="Top 10 Wealthiest Users", color=discord.Color.gold())

        for user_id, total_wealth in top_users:
            username = self.get_username(ctx, user_id)

            # Format wealth in shorthand
            wealth_shorthand = self.format_value(total_wealth)

            embed.add_field(name=username, value=wealth_shorthand, inline=False)

        await ctx.send(embed=embed)


## Wealth Functions

    def get_username(self, ctx, user_id):
        user = ctx.guild.get_member(user_id)
        if user:
            return user.display_name
        else:
            return f"User ID: {user_id}"

    def calculate_total_wealth(self, user_id):
        cursor = self.conn.cursor()

        # Get user balance
        cursor.execute("SELECT balance FROM users WHERE user_id=?", (user_id,))
        current_balance_row = cursor.fetchone()
        current_balance = current_balance_row[0] if current_balance_row else 0

        # Calculate total stock value
        cursor.execute("SELECT symbol, amount FROM user_stocks WHERE user_id=?", (user_id,))
        user_stocks = cursor.fetchall()
        total_stock_value = 0

        for symbol, amount in user_stocks:
            cursor.execute("SELECT price FROM stocks WHERE symbol=?", (symbol,))
            stock_price_row = cursor.fetchone()
            stock_price = stock_price_row[0] if stock_price_row else 0
            total_stock_value += stock_price * amount

        # Calculate total ETF value
        cursor.execute("SELECT etf_id, quantity FROM user_etfs WHERE user_id=?", (user_id,))
        user_etfs = cursor.fetchall()
        total_etf_value = 0

        for etf in user_etfs:
            etf_id = etf[0]
            quantity = etf[1]

            cursor.execute("SELECT SUM(stocks.price * etf_stocks.quantity) FROM etf_stocks JOIN stocks ON etf_stocks.symbol = stocks.symbol WHERE etf_stocks.etf_id=? GROUP BY etf_stocks.etf_id", (etf_id,))
            etf_value_row = cursor.fetchone()
            etf_value = etf_value_row[0] if etf_value_row else 0
            total_etf_value += (etf_value or 0) * quantity

        return current_balance + total_stock_value + total_etf_value

    def format_value(self, value):
        if value >= 10 ** 15:
            return f"{value / 10 ** 15:.2f}Q"
        elif value >= 10 ** 12:
            return f"{value / 10 ** 12:.2f}T"
        elif value >= 10 ** 9:
            return f"{value / 10 ** 9:.2f}B"
        elif value >= 10 ** 6:
            return f"{value / 10 ** 6:.2f}M"
        else:
            return f"{value:.2f}"
## End Wealth Functions




    @commands.command(name="total_tax_distributed", help="View the total amount of tax distributed.")
    async def total_tax_distributed(self, ctx):
        cursor = self.conn.cursor()
        cursor.execute("SELECT metric_value FROM metrics WHERE metric_name = 'total_tax_distributed'")
        result = cursor.fetchone()
        total_tax_distributed = Decimal(result[0]) if result else Decimal(0)

        # Format the total tax distributed value with commas
        formatted_total_tax_distributed = "{:,.2f}".format(total_tax_distributed)

        # Create an embedded message
        embed = discord.Embed(title="Total Tax Distributed", color=discord.Color.green())
        embed.add_field(name="Amount", value=f"{formatted_total_tax_distributed} coins", inline=False)

        # Send the embedded message
        await ctx.send(embed=embed)

    @commands.command(name="metrics", help="View system metrics")
    @is_allowed_server(P3, SludgeSliders, OM3, PBL)
    async def view_metrics(self, ctx):
        cursor = self.conn.cursor()

        try:
            cursor.execute("SELECT * FROM metrics")
            metrics = cursor.fetchall()

            if not metrics:
                await ctx.send("No metrics found.")
                return

            metrics_str = "\n".join([f"{metric[0]}: {metric[1]}" for metric in metrics])
            await ctx.send(f"System Metrics:\n{metrics_str}")
        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while retrieving system metrics. Error: {str(e)}")

    @commands.command(name='etf_metric', help='Show the current value and top holders of an ETF.')
    async def etf_metric(self, ctx, etf_id: int):
        try:
            cursor = self.conn.cursor()

            # Get the current value of the ETF
            cursor.execute("SELECT value FROM etfs WHERE etf_id = ?", (etf_id,))
            result = cursor.fetchone()
            if result:
                etf_value = result[0]
                await ctx.send(f"Current value of ETF with ID {etf_id}: {etf_value} coins")
            else:
                await ctx.send(f"ETF with ID {etf_id} does not exist.")

            # Get the top holders of the ETF
            cursor.execute("""
                SELECT user_id, quantity
                FROM user_etfs
                WHERE etf_id = ?
                ORDER BY quantity DESC
                LIMIT 5
            """, (etf_id,))
            top_holders = cursor.fetchall()

            if top_holders:
                top_holders_str = "\n".join([f"<@{user_id}> - {quantity} shares" for user_id, quantity in top_holders])
                await ctx.send(f"Top holders of ETF with ID {etf_id}:\n{top_holders_str}")
            else:
                await ctx.send(f"No one currently holds shares in ETF with ID {etf_id}.")

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while fetching ETF information: {e}")


# Currency Tools

    @commands.command(name="reset_game", help="Reset all user stocks and ETFs to 0 and set the balance to 100.00")
    @is_allowed_server(P3, SludgeSliders, OM3, PBL)
    @is_allowed_user(930513222820331590, PBot)
    async def reset_game(self, ctx):
        cursor = self.conn.cursor()

        # Reset user stocks and ETFs to 0
        try:
            cursor.execute("DELETE FROM user_stocks")
            cursor.execute("DELETE FROM user_etfs")
        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while resetting user stocks and ETFs. Error: {str(e)}")
            return

        # Set balance to 100,000.00 for all users
        try:
            cursor.execute("UPDATE users SET balance = '100.00'")
        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while updating user balance. Error: {str(e)}")
            return

        self.conn.commit()
        await ctx.send("All user stocks and ETFs have been reset to 0, and the balance is set to 100,000.00 for all users.")


    @commands.command(name="revert_all_stocks", help="Revert all user-held stocks and place them back into the stock market for all users.")
    @is_allowed_server(P3, SludgeSliders, OM3, PBL)
    @is_allowed_user(930513222820331590, PBot)
    async def revert_all_stocks(self, ctx):
        cursor = self.conn.cursor()

        # Fetch all user-held stocks from the user_stocks table
        cursor.execute("SELECT * FROM user_stocks")
        user_stocks = cursor.fetchall()

        if not user_stocks:
            await ctx.send("No user-held stocks found.")
            return

        try:
            # Move the user-held stocks back to the stock market
            for stock in user_stocks:
                user_id = stock[0]
                symbol = stock[1]
                amount = stock[2]

                # Update the stock's available amount in the stocks table
                cursor.execute("UPDATE stocks SET available = available + ? WHERE symbol = ?", (amount, symbol))

            # Delete all user's stock holdings
            cursor.execute("DELETE FROM user_stocks")

            self.conn.commit()
            await ctx.send("All user-held stocks have been successfully reverted and placed back into the stock market.")
        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while reverting the stocks: {e}")

    @commands.command(name="revert_stocks", help="Revert all user-held stocks and place them back into the stock market.")
    @is_allowed_user(930513222820331590, PBot)
    async def revert_stocks(self, ctx):
        user_id = ctx.author.id
        cursor = self.conn.cursor()

        # Fetch all the stocks held by the user
        cursor.execute("SELECT * FROM user_stocks WHERE user_id=?", (user_id,))
        user_stocks = cursor.fetchall()

        if not user_stocks:
            await ctx.send("You don't hold any stocks to revert.")
            return

        try:
            # Move the user-held stocks back to the stock market
            for stock in user_stocks:
                symbol = stock[1]
                amount = stock[2]

                # Update the stock's available amount in the stocks table
                cursor.execute("UPDATE stocks SET available = available + ? WHERE symbol = ?", (amount, symbol))

            # Delete the user's stock holdings
            cursor.execute("DELETE FROM user_stocks WHERE user_id=?", (user_id,))

            self.conn.commit()
            await ctx.send("All your held stocks have been successfully reverted and placed back into the stock market.")
        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while reverting the stocks: {e}")


    @commands.command(name="give", help="Give a specified amount of coins to another user.")
    async def give(self, ctx, recipient: discord.Member, amount: int):
        if amount <= 0:
            await ctx.send("The amount to give should be greater than 0.")
            return

        sender_id = ctx.author.id
        recipient_id = recipient.id
        sender_balance = get_user_balance(self.conn, sender_id)

        if sender_balance < amount:
            await ctx.send(f"{ctx.author.mention}, you don't have enough coins to give. Your current balance is {sender_balance} coins.")
            return

        # Deduct the amount from the sender's balance
        update_user_balance(self.conn, sender_id, sender_balance - amount)

        # Add the amount to the recipient's balance
        recipient_balance = get_user_balance(self.conn, recipient_id)
        update_user_balance(self.conn, recipient_id, recipient_balance + amount)

        # Log the transfer
        await log_transfer(ledger_conn, ctx, ctx.author.name, recipient.name, recipient.id, amount)


        await ctx.send(f"{ctx.author.mention}, you have successfully given {amount} coins to {recipient.mention}.")

    @commands.command(name="daily", help="Claim your daily coins.")
    @is_allowed_server(P3, SludgeSliders, OM3, PBL)
    async def daily(self, ctx):
        async with self.lock:  # Use the asynchronous lock
            user_id = ctx.author.id
            current_time = datetime.now()

            # If the user hasn't claimed or 24 hours have passed since the last claim
            if user_id not in self.last_claimed or (current_time - self.last_claimed[user_id]).total_seconds() > 86400:
                amount = random.randint(dailyMin, dailyMax)
                current_balance = get_user_balance(self.conn, user_id)
                new_balance = current_balance + amount
                update_user_balance(self.conn, user_id, new_balance)
                await ctx.send(f"{ctx.author.mention}, you have claimed {amount} coins. Your new balance is: {new_balance} coins.")
                self.last_claimed[user_id] = current_time  # Update the last claimed time
            else:
                time_left = 86400 - (current_time - self.last_claimed[user_id]).total_seconds()
                hours, remainder = divmod(time_left, 3600)
                minutes, _ = divmod(remainder, 60)
                await ctx.send(f"You've already claimed your daily reward! You can claim again in {int(hours)} hours and {int(minutes)} minutes.")




    @commands.command(name="balance", aliases=["wallet"], help="Check your balance.")
    async def balance(self, ctx):
        await ctx.message.delete()
        user_id = ctx.author.id
        balance = get_user_balance(self.conn, user_id)

        # Format balance with commas and make it more visually appealing
        formatted_balance = "{:,}".format(balance)
        formatted_balance = f"**{formatted_balance}** coins"

        embed = discord.Embed(
            title="Balance",
            description=f"Balance for {ctx.author.name}:",
            color=discord.Color.blue()
        )
        embed.set_thumbnail(url="https://mirror.xyz/_next/image?url=https%3A%2F%2Fimages.mirror-media.xyz%2Fpublication-images%2F8XKxIUMy9CE8zg54-ZsP3.png&w=828&q=75")  # Add your own coin icon URL
        embed.add_field(name="Coins", value=formatted_balance, inline=False)
        embed.set_footer(text="Thank you for using our currency system!")

        await ctx.send(embed=embed)

    @commands.command(name="add")
    @is_allowed_user(930513222820331590, PBot)
    async def add(self, ctx, amount: int):
        await ctx.message.delete()
        user_id = ctx.author.id
        current_balance = get_user_balance(self.conn, user_id)
        new_balance = current_balance + amount
        update_user_balance(self.conn, user_id, new_balance)
        await ctx.send(f"{ctx.author.mention}, you have added {amount} coins. Your new balance is: {new_balance} coins.")



    @commands.command(name="reset_user_coins")
    @is_allowed_user(930513222820331590, PBot)
    async def reset_user_coins(self, ctx):
        try:
            # Set user balance to 10,000 coins
            cursor = self.conn.cursor()
            cursor.execute("UPDATE users SET balance = ?", (resetCoins,))

            self.conn.commit()
            await ctx.send("User coins reset successfully.")

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while resetting user coins: {e}")

    @commands.command(name="distribute_tokens", help="Distribute tokens to all users in the server.")
    @is_allowed_user(930513222820331590, PBot)  # Replace with your allowed user IDs
    async def distribute_tokens(self, ctx, amount: int):
        guild = ctx.guild

        if amount <= 0:
            await ctx.send("Invalid amount. Please provide a positive number of tokens.")
            return

        # Distribute tokens to all members in the server
        for member in guild.members:
            if not member.bot:
                # Update user balance
                try:
                    update_user_balance(self.conn, member.id, amount)
                except ValueError as e:
                    await ctx.send(f"An error occurred while updating the balance for user {member.name}: {str(e)}")

        await ctx.send(f"Distributed {amount} tokens to all users in the server.")



# Debug Start

    @commands.command(name="reset_users")
    @is_allowed_user(930513222820331590, PBot)
    async def reset_users(self, ctx):
        try:
            cursor = self.conn.cursor()

            # Reset user balances to 10,000 coins for all users
            cursor.execute("UPDATE users SET balance = 10000")

            # Clear user stocks
            cursor.execute("DELETE FROM user_stocks")

            # Reset available amounts of stocks to their total supplies
            cursor.execute("UPDATE stocks SET available = total_supply")

            # Clear user commodities
            cursor.execute("DELETE FROM user_commodities")

            # Reset available amounts of commodities to their total supplies
            cursor.execute("UPDATE commodities SET available = total_supply")

            self.conn.commit()
            await ctx.send("User balances, stocks, and commodities reset successfully for all users.")

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while resetting user balances: {e}")



    @commands.command(name="backup_database")
    @is_allowed_server(P3, SludgeSliders, OM3, PBL)
    @is_allowed_user(930513222820331590, PBot)
    async def backup_database(self, ctx):
        try:
            # Create a timestamp for the backup file name
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

            # Specify the source file path (current directory + database file)
            source_path = "currency_system.db"

            # Specify the destination file path (backup file location with timestamp)
            destination_path = f"backup_{timestamp}.db"

            # Create a backup of the database file
            shutil.copy2(source_path, destination_path)

            await ctx.send(f"Database backup created: {destination_path}")

        except Exception as e:
            await ctx.send(f"An error occurred while creating the database backup: {e}")

    @commands.command(name="restore_backup")
    @is_allowed_server(P3, SludgeSliders, OM3, PBL)
    @is_allowed_user(930513222820331590, PBot)
    async def restore_database(self, ctx):
        try:
            # Specify the backup file path pattern
            backup_path_pattern = "backup_*.db"

            # Get the list of backup files sorted by modification time
            backup_files = sorted(glob.glob(backup_path_pattern), key=os.path.getmtime, reverse=True)

            if len(backup_files) > 0:
                # Select the most recent backup file
                latest_backup_file = backup_files[0]

                # Specify the destination file path (current directory + database file)
                destination_path = "currency_system.db"

                # Restore the database from the most recent backup file
                shutil.copy2(latest_backup_file, destination_path)

                await ctx.send(f"Database restored from backup: {latest_backup_file}")
            else:
                await ctx.send("No backup files found.")

        except Exception as e:
            await ctx.send(f"An error occurred while restoring the database: {e}")


    @commands.command(name="historical_data", help="View historical data of a stock. Provide the stock name.")
    async def historical_data(self, ctx, stock_name: str):
        data = view_historical_data(self.conn, stock_name)
        await ctx.send(f"```\n{data}\n```")

    @commands.command(name="stock_stats", help="Get total buys, total sells, average price, and current price of a stock.")
    async def stock_stats(self, ctx, symbol: str):
        # Retrieve relevant transactions for the specified stock symbol from the database
        cursor = ledger_conn.cursor()
        cursor.execute("""
            SELECT action, quantity, price
            FROM stock_transactions
            WHERE symbol=? AND (action='Buy Stock' OR action='Sell Stock')
        """, (symbol,))
        
        transactions = cursor.fetchall()

        # Initialize variables to calculate statistics
        total_buys = 0
        total_sells = 0
        total_quantity_buys = 0
        total_quantity_sells = 0

        for action, quantity, price_str in transactions:
            price = float(price_str)

            if action == 'Buy Stock':
                total_buys += quantity * price
                total_quantity_buys += quantity
            elif action == 'Sell Stock':
                total_sells += quantity * price
                total_quantity_sells += quantity

        # Calculate average price
        average_price = total_buys / total_quantity_buys if total_quantity_buys > 0 else 0

        # Connect to currency_system.db and fetch the current price
        currency_conn = sqlite3.connect("currency_system.db")
        cursor_currency = currency_conn.cursor()
        cursor_currency.execute("SELECT available, price FROM stocks WHERE symbol = ?", (symbol,))
        result = cursor_currency.fetchone()
        currency_conn.close()

        if result:
            available, current_price = result
            formatted_current_price = '{:,.2f}'.format(current_price)
            await ctx.send(f"Current price of {symbol}: {formatted_current_price} coins\nAvailable supply: {available}")
        else:
            await ctx.send(f"{symbol} is not a valid stock symbol.")

        # Determine if it's over or undervalued
        valuation_label = "Overvalued" if current_price > average_price else "Undervalued"

        # Format totals with commas
        formatted_total_buys = '{:,}'.format(int(total_buys))
        formatted_total_sells = '{:,}'.format(int(total_sells))

        # Create an embed to display the statistics
        embed = discord.Embed(
            title=f"Stock Statistics for {symbol}",
            color=discord.Color.blue()
        )
        embed.add_field(name="Total Buys", value=f"{formatted_total_buys} coins ({total_quantity_buys:,} buys)", inline=False)
        embed.add_field(name="Total Sells", value=f"{formatted_total_sells} coins ({total_quantity_sells:,} sells)", inline=False)
        embed.add_field(name="Average Price", value=f"{average_price:.2f} coins", inline=False)
        embed.add_field(name="Valuation", value=valuation_label, inline=False)

        await ctx.send(embed=embed)


    @commands.command(name="etf_stats", help="Get total buys, total sells, average price, and current price of an ETF.")
    async def etf_stats(self, ctx, etf_id: int):
        try:
            # Connect to the ledger database
            ledger_conn = sqlite3.connect("p3ledger.db")
            ledger_cursor = ledger_conn.cursor()

            # Initialize variables to calculate statistics
            total_buys = 0
            total_sells = 0
            total_quantity_buys = 0
            total_quantity_sells = 0

            # Retrieve relevant transactions for the specified ETF ID from the ledger database
            ledger_cursor.execute("""
                SELECT action, quantity, price
                FROM stock_transactions
                WHERE symbol=? AND (action='Buy ETF' OR action='Sell ETF')
            """, (etf_id,))

            transactions = ledger_cursor.fetchall()

            for action, quantity, price_str in transactions:
                price = float(price_str)

                if action == 'Buy ETF':
                    total_buys += quantity * price
                    total_quantity_buys += quantity
                elif action == 'Sell ETF':
                    total_sells += quantity * price
                    total_quantity_sells += quantity

            # Calculate average price
            average_price = total_buys / total_quantity_buys if total_quantity_buys > 0 else 0

            # Connect to the currency_system database
            currency_conn = sqlite3.connect("currency_system.db")
            currency_cursor = currency_conn.cursor()

            # Retrieve the ETF's name and description
            currency_cursor.execute("""
                SELECT name, description
                FROM etfs
                WHERE etf_id = ?
            """, (etf_id,))

            etf_info = currency_cursor.fetchone()

            if etf_info:
                etf_name, etf_description = etf_info

                # Retrieve the list of underlying stocks and their quantities
                currency_cursor.execute("""
                    SELECT symbol, quantity
                    FROM etf_stocks
                    WHERE etf_id = ?
                """, (etf_id,))

                stocks = currency_cursor.fetchall()

                # Calculate the current value of the ETF
                etf_value = 0
                for stock in stocks:
                    symbol, quantity = stock

                    # Retrieve the price of the underlying stock from the currency_system database
                    currency_cursor.execute("""
                        SELECT price
                        FROM stocks
                        WHERE symbol = ?
                    """, (symbol,))

                    stock_price = currency_cursor.fetchone()

                    if stock_price:
                        etf_value += stock_price[0] * quantity

                # Determine if the ETF is overvalued or undervalued
                valuation_label = "Overvalued" if etf_value > average_price else "Undervalued"

                # Format the values with commas
                formatted_total_buys = '{:,}'.format(int(total_buys))
                formatted_total_sells = '{:,}'.format(int(total_sells))
                formatted_average_price = '{:,.2f}'.format(average_price)
                formatted_etf_value = '{:,.2f}'.format(etf_value)

                # Create an embed to display the statistics
                embed = discord.Embed(
                    title=f"ETF Statistics for ETF {etf_id}",
                    description=f"Name: {etf_name}\nDescription: {etf_description}\n",
                    color=discord.Color.blue()
                )
                embed.add_field(name="Total Buys", value=f"{formatted_total_buys} coins ({total_quantity_buys:,} buys)", inline=False)
                embed.add_field(name="Total Sells", value=f"{formatted_total_sells} coins ({total_quantity_sells:,} sells)", inline=False)
                embed.add_field(name="Average Price", value=f"{formatted_average_price} coins", inline=False)
                embed.add_field(name="Current ETF Value", value=f"{formatted_etf_value} coins", inline=False)
                embed.add_field(name="Valuation", value=valuation_label, inline=False)

                await ctx.send(embed=embed)

            else:
                await ctx.send(f"ETF {etf_id} not found.")

            # Close the database connections
            ledger_conn.close()
            currency_conn.close()

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred: {str(e)}")


    @commands.command(name="market_stats", help="Get market statistics including ETFs, stocks, and top undervalued stocks.")
    async def market_stats(self, ctx):  # Add 'self' parameter here
        try:
            # Connect to the currency_system database
            currency_conn = sqlite3.connect("currency_system.db")
            currency_cursor = currency_conn.cursor()

            # Connect to the ledger database
            ledger_conn = sqlite3.connect("p3ledger.db")
            ledger_cursor = ledger_conn.cursor()

            # Calculate the total value of ETFs
            currency_cursor.execute("SELECT etf_id, name FROM etfs")
            etfs = currency_cursor.fetchall()
            total_etf_value = 0

            for etf in etfs:
                etf_id, etf_name = etf
                currency_cursor.execute("SELECT symbol, quantity FROM etf_stocks WHERE etf_id=?", (etf_id,))
                stocks = currency_cursor.fetchall()

                etf_value = 0
                for stock in stocks:
                    symbol, quantity = stock
                    currency_cursor.execute("SELECT price FROM stocks WHERE symbol=?", (symbol,))
                    stock_price = currency_cursor.fetchone()

                    if stock_price:
                        etf_value += stock_price[0] * quantity

                total_etf_value += etf_value

            # Calculate the total value of all stocks
            currency_cursor.execute("SELECT symbol, price, available FROM stocks")
            stocks = currency_cursor.fetchall()
            total_stock_value = sum(stock[1] * stock[2] for stock in stocks)

            # Calculate the total market value
            total_market_value = total_etf_value + total_stock_value

            # Calculate the top 5 undervalued stocks
            currency_cursor.execute("SELECT symbol, price, available FROM stocks ORDER BY price / (price + 1) LIMIT 5")
            undervalued_stocks = currency_cursor.fetchall()

            # Calculate the best ETF to buy
            best_etf_id = None
            best_etf_value = float('inf')  # Initialize with positive infinity
            best_etf_name = "None"  # Default value when no best ETF is found

            for etf in etfs:
                etf_id, etf_name = etf
                currency_cursor.execute("SELECT symbol, quantity FROM etf_stocks WHERE etf_id=?", (etf_id,))
                stocks = currency_cursor.fetchall()

                etf_value = 0
                for stock in stocks:
                    symbol, quantity = stock
                    currency_cursor.execute("SELECT price FROM stocks WHERE symbol=?", (symbol,))
                    stock_price = currency_cursor.fetchone()

                    if stock_price:
                        etf_value += stock_price[0] * quantity

                if etf_value < best_etf_value:
                    best_etf_id = etf_id
                    best_etf_value = etf_value
                    best_etf_name = etf_name

            # Format the values with commas and create an embed
            formatted_total_etf_value = '{:,.2f}'.format(total_etf_value)
            formatted_total_stock_value = '{:,.2f}'.format(total_stock_value)
            formatted_undervalued_stocks = "\n".join([f"{stock[0]}: {'{:,.2f}'.format(stock[1])} coins" for stock in undervalued_stocks])
            formatted_best_etf_id = best_etf_id if best_etf_id else "None"
            formatted_best_etf_value = '{:,.2f}'.format(best_etf_value)

            # Create an embed to display the market statistics
            embed = discord.Embed(
                title="Market Statistics",
                color=discord.Color.blue()
            )
            embed.add_field(name="Total ETF Value", value=f"{formatted_total_etf_value} coins", inline=False)
            embed.add_field(name="Total Stock Value", value=f"{formatted_total_stock_value} coins", inline=False)
            embed.add_field(name="Top 5 Undervalued Stocks", value=formatted_undervalued_stocks, inline=False)
            embed.add_field(name="Best ETF to Buy", value=f"ETF {formatted_best_etf_id} ({best_etf_name}) with a value of {formatted_best_etf_value} coins", inline=False)

            # Send the embed as a message
            await ctx.send(embed=embed)

            # Close the database connections
            currency_conn.close()
            ledger_conn.close()

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred: {str(e)}")










# Stock Market
# Buy Stock
    @commands.command(name="buy", aliases=["buy_stock"], help="Buy stocks. Provide the stock name and amount.")
    @is_allowed_server(P3, SludgeSliders, OM3, PBL)
    async def buy(self, ctx, stock_name: str, amount: int):
        user_id = ctx.author.id

        await ctx.message.delete()
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
        stock = cursor.fetchone()
        if stock is None:
            await ctx.send(f"{ctx.author.mention}, this stock does not exist.")
            return

        # Get the total amount bought today by the user for this stock
        cursor.execute("""
            SELECT SUM(amount), MAX(timestamp)
            FROM user_daily_buys
            WHERE user_id=? AND symbol=? AND DATE(timestamp)=DATE('now')
        """, (user_id, stock_name))

        daily_bought_record = cursor.fetchone()
        daily_bought = daily_bought_record[0] if daily_bought_record and daily_bought_record[0] is not None else 0
        last_purchase_time = daily_bought_record[1] if daily_bought_record and daily_bought_record[1] is not None else None

        if daily_bought + amount > dStockLimit:
            remaining_amount = dStockLimit - daily_bought

            # Calculate the time remaining until they can buy again
            if last_purchase_time:
                last_purchase_datetime = datetime.strptime(last_purchase_time, '%Y-%m-%d %H:%M:%S')
                time_until_reset = (last_purchase_datetime + timedelta(days=1)) - datetime.now()
                hours, remainder = divmod(time_until_reset.seconds, 3600)
                minutes, _ = divmod(remainder, 60)
                time_remaining_str = f"{hours} hours and {minutes} minutes"
            else:
                time_remaining_str = "24 hours"

            await ctx.send(f"{ctx.author.mention}, you have reached your daily buy limit for {stock_name} stocks. "
                           f"You can buy {remaining_amount} more after {time_remaining_str}.")
            return

        available, price, total_supply = int(stock[4]), Decimal(stock[2]), int(stock[3])  # Assuming stock[3] is total supply

        # Check the existing amount of this stock owned by the user
        cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (user_id, stock_name))
        user_stock = cursor.fetchone()
        user_owned = int(user_stock[0]) if user_stock else 0

        if (user_owned + amount) > (total_supply * 0.51):
            await ctx.send(f"{ctx.author.mention}, you cannot own more than 51% of the total supply of {stock_name} stocks.")
            return

        if amount > available:
            await ctx.send(f"{ctx.author.mention}, there are only {available} {stock_name} stocks available.")
            return

        cost = price * Decimal(amount)
        tax_percentage = get_tax_percentage(amount, cost)
        fee = cost * Decimal(tax_percentage)
        total_cost = cost + fee

        current_balance = get_user_balance(self.conn, user_id)

        if total_cost > current_balance:
            # Calculate the missing amount needed to complete the transaction including tax.
            missing_amount = total_cost - current_balance
            await ctx.send(f"{ctx.author.mention}, you do not have enough coins to buy these stocks. You need {missing_amount:.2f} more coins, including tax, to complete this purchase.")
            return

        # Update the user's balance
        new_balance = current_balance - total_cost
        try:
            update_user_balance(self.conn, user_id, new_balance)
        except ValueError as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while updating the user balance. Error: {str(e)}")
            return

        # Update user's stocks
        try:
            cursor.execute("""
                INSERT INTO user_stocks (user_id, symbol, amount)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id, symbol) DO UPDATE SET amount = amount + ?
            """, (user_id, stock_name, amount, amount))
        except sqlite3.Error as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while updating user stocks. Error: {str(e)}")
            return

        # Update available stock
        try:
            cursor.execute("""
                UPDATE stocks
                SET available = available - ?
                WHERE symbol = ?
            """, (amount, stock_name))
        except sqlite3.Error as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while updating available stocks. Error: {str(e)}")
            return

        # Record this transaction in the user_daily_buys table
        try:
            cursor.execute("""
                INSERT INTO user_daily_buys (user_id, symbol, amount, timestamp)
                VALUES (?, ?, ?, datetime('now'))
            """, (user_id, stock_name, amount))
        except sqlite3.Error as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while updating daily stock limit. Error: {str(e)}")
            return

        # Check if the user is part of a trading team
        cursor.execute("SELECT team_id FROM team_members WHERE user_id=?", (user_id,))
        team_record = cursor.fetchone()
        if team_record:
            team_id = team_record[0]
            # Record the team's transaction
            record_team_transaction(self.conn, team_id, stock_name, amount, price, "buy")
            # Calculate and update the team's profit/loss
            calculate_team_profit_loss(self.conn, team_id)


        decay_other_stocks(self.conn, stock_name)
        await self.increase_price(ctx, stock_name, amount)
        await log_transaction(ledger_conn, ctx, "Buy Stock", stock_name, amount, cost, total_cost, current_balance, new_balance, price)
        self.conn.commit()

#        await update_market_etf_price(bot, conn)
        await ctx.send(f"{ctx.author.mention}, you have bought {amount} {stock_name} stocks. Your new balance is: {new_balance} coins.")



# Sell Stock
    @commands.command(name="sell", aliases=["sell_stock"], help="Sell stocks. Provide the stock name and amount.")
    @is_allowed_server(P3, SludgeSliders, OM3, PBL)
    async def sell(self, ctx, stock_name: str, amount: int):
        user_id = ctx.author.id

        await ctx.message.delete()
        cursor = self.conn.cursor()

        cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (user_id, stock_name))
        user_stock = cursor.fetchone()
        if not user_stock or int(user_stock[0]) < amount:
            await ctx.send(f"{ctx.author.mention}, you do not have enough {stock_name} stocks to sell.")
            return

        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
        stock = cursor.fetchone()
        if stock is None:
            await ctx.send(f"{ctx.author.mention}, this stock does not exist.")
            return

        price = Decimal(stock[2])
        earnings = price * Decimal(amount)
        tax_percentage = get_tax_percentage(amount, earnings)  # Custom function to determine the tax percentage based on quantity and earnings
        fee = earnings * Decimal(tax_percentage)
        total_earnings = earnings - fee

        current_balance = get_user_balance(self.conn, user_id)
        new_balance = current_balance + total_earnings

        try:
            update_user_balance(self.conn, user_id, new_balance)
        except ValueError as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while updating the user balance. Error: {str(e)}")
            return

        cursor.execute("""
            UPDATE user_stocks
            SET amount = amount - ?
            WHERE user_id = ? AND symbol = ?
        """, (amount, user_id, stock_name))

        cursor.execute("""
            UPDATE stocks
            SET available = available + ?
            WHERE symbol = ?
        """, (amount, stock_name))
        decay_other_stocks(self.conn, stock_name)

        # Check if the user is part of a trading team
        cursor.execute("SELECT team_id FROM team_members WHERE user_id=?", (user_id,))
        team_record = cursor.fetchone()
        if team_record:
            team_id = team_record[0]
            # Record the team's transaction
            record_team_transaction(self.conn, team_id, stock_name, amount, price, "sell")
            # Calculate and update the team's profit/loss
            calculate_team_profit_loss(self.conn, team_id)

        await self.decrease_price(ctx, stock_name, amount)
        await log_transaction(ledger_conn, ctx, "Sell Stock", stock_name, amount, earnings, total_earnings, current_balance, new_balance, price)
        self.conn.commit()

#        await update_market_etf_price(bot, conn)
        await ctx.send(f"{ctx.author.mention}, you have sold {amount} {stock_name} stocks. Your new balance is: {new_balance} coins.")


# Buy/Sell Multi



    @commands.command(name="buyMulti", aliases=["buy_multi"], help="Buy stocks for all stocks in the market. Provide the amount to buy.")
    @is_allowed_server(P3, SludgeSliders, OM3, PBL)
    async def buy_multi(self, ctx, amount: int):
        user_id = ctx.author.id

        user_id = ctx.author.id
        current_time = datetime.now()

        # Check if the user has run the command before and if enough time has passed
        if user_id in last_buy_time:
            last_time = last_buy_time[user_id]
            time_elapsed = current_time - last_time
            cooldown_duration = timedelta(minutes=1440)

            if time_elapsed < cooldown_duration:
                # Calculate the remaining cooldown time
                remaining_time = cooldown_duration - time_elapsed
                await ctx.send(f"{ctx.author.mention}, you can use this command again in {remaining_time}.")

                return

        await ctx.message.delete()
        cursor = self.conn.cursor()

        # Get a list of all stocks in the market
        cursor.execute("SELECT symbol FROM stocks")
        market_stocks = cursor.fetchall()

        for stock in market_stocks:
            stock_name = stock[0]

            # Check if the stock exists
            cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
            stock_info = cursor.fetchone()
            if stock_info is None:
                await ctx.send(f"{ctx.author.mention}, stock '{stock_name}' does not exist.")
                continue

            # Your existing buy logic
            available, price, total_supply = int(stock_info[4]), Decimal(stock_info[2]), int(stock_info[3])

            # Check the existing amount bought today by the user for this stock
            cursor.execute("""
                SELECT SUM(amount), MAX(timestamp)
                FROM user_daily_buys
                WHERE user_id=? AND symbol=? AND DATE(timestamp)=DATE('now')
            """, (user_id, stock_name))

            daily_bought_record = cursor.fetchone()
            daily_bought = daily_bought_record[0] if daily_bought_record and daily_bought_record[0] is not None else 0
            last_purchase_time = daily_bought_record[1] if daily_bought_record and daily_bought_record[1] is not None else None

            if daily_bought + amount > dStockLimit:
                remaining_amount = dStockLimit - daily_bought

                # Calculate the time remaining until they can buy again
                if last_purchase_time:
                    last_purchase_datetime = datetime.strptime(last_purchase_time, '%Y-%m-%d %H:%M:%S')
                    time_until_reset = (last_purchase_datetime + timedelta(days=1)) - datetime.now()
                    hours, remainder = divmod(time_until_reset.seconds, 3600)
                    minutes, _ = divmod(remainder, 60)
                    time_remaining_str = f"{hours} hours and {minutes} minutes"
                else:
                    time_remaining_str = "24 hours"

                await ctx.send(f"{ctx.author.mention}, you have reached your daily buy limit for {stock_name} stocks. "
                               f"You can buy {remaining_amount} more after {time_remaining_str}.")
                continue

            if amount > available:
                await ctx.send(f"{ctx.author.mention}, there are only {available} {stock_name} stocks available for purchase.")
                continue

            cost = price * Decimal(amount)
            tax_percentage = get_tax_percentage(amount, cost)
            fee = cost * Decimal(tax_percentage)
            total_cost = cost + fee

            current_balance = get_user_balance(self.conn, user_id)

            if total_cost > current_balance:
                # Calculate the missing amount needed to complete the transaction including tax.
                missing_amount = total_cost - current_balance
                await ctx.send(f"{ctx.author.mention}, you do not have enough coins to buy {amount} {stock_name} stocks. "
                               f"You need {missing_amount:.2f} more coins, including tax, to complete this purchase.")
                continue

            # Update the user's balance
            new_balance = current_balance - total_cost
            try:
                update_user_balance(self.conn, user_id, new_balance)
            except ValueError as e:
                await ctx.send(f"{ctx.author.mention}, an error occurred while updating the user balance. Error: {str(e)}")
                continue

            # Update user's stocks
            try:
                cursor.execute("""
                    INSERT INTO user_stocks (user_id, symbol, amount)
                    VALUES (?, ?, ?)
                    ON CONFLICT(user_id, symbol) DO UPDATE SET amount = amount + ?
                """, (user_id, stock_name, amount, amount))
            except sqlite3.Error as e:
                await ctx.send(f"{ctx.author.mention}, an error occurred while updating user stocks. Error: {str(e)}")
                continue

            # Update available stock
            try:
                cursor.execute("""
                    UPDATE stocks
                    SET available = available - ?
                    WHERE symbol = ?
                """, (amount, stock_name))
            except sqlite3.Error as e:
                await ctx.send(f"{ctx.author.mention}, an error occurred while updating available stocks. Error: {str(e)}")
                continue

            # Record this transaction in the user_daily_buys table
            try:
                cursor.execute("""
                    INSERT INTO user_daily_buys (user_id, symbol, amount, timestamp)
                    VALUES (?, ?, ?, datetime('now'))
                """, (user_id, stock_name, amount))
            except sqlite3.Error as e:
                await ctx.send(f"{ctx.author.mention}, an error occurred while updating daily stock limit. Error: {str(e)}")
                continue

            # Check if the user is part of a trading team
            cursor.execute("SELECT team_id FROM team_members WHERE user_id=?", (user_id,))
            team_record = cursor.fetchone()
            if team_record:
                team_id = team_record[0]
                # Record the team's transaction
                record_team_transaction(self.conn, team_id, stock_name, amount, price, "buy")
                # Calculate and update the team's profit/loss
                calculate_team_profit_loss(self.conn, team_id)

            decay_other_stocks(self.conn, stock_name)
            await self.increase_price(ctx, stock_name, amount)
            await log_transaction(ledger_conn, ctx, "Buy Stock", stock_name, amount, cost, total_cost, current_balance, new_balance, price)
            last_buy_time[user_id] = current_time
            self.conn.commit()

    @commands.command(name="sellMulti", aliases=["sell_multi"], help="Sell stocks for all stocks in the market. Provide the amount to sell.")
    @is_allowed_server(P3, SludgeSliders, OM3, PBL)
    async def sell_multi(self, ctx, amount: int):
        user_id = ctx.author.id
        current_time = datetime.now()

        # Check if the user has run the command before and if enough time has passed
        if user_id in last_sell_time:
            last_time = last_sell_time[user_id]
            time_elapsed = current_time - last_time
            cooldown_duration = timedelta(minutes=1440)

            if time_elapsed < cooldown_duration:
                # Calculate the remaining cooldown time
                remaining_time = cooldown_duration - time_elapsed
                await ctx.send(f"{ctx.author.mention}, you can use this command again in {remaining_time}.")

                return

        await ctx.message.delete()
        cursor = self.conn.cursor()

        # Get a list of all stocks in the market
        cursor.execute("SELECT symbol FROM stocks")
        market_stocks = cursor.fetchall()

        for stock in market_stocks:
            stock_name = stock[0]

            # Check if the user owns any of this stock
            cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (user_id, stock_name))
            user_stock = cursor.fetchone()
            if not user_stock or int(user_stock[0]) < amount:
                continue

            # Your existing sell logic here...
            cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
            stock_info = cursor.fetchone()
            if stock_info is None:
                await ctx.send(f"{ctx.author.mention}, this stock '{stock_name}' does not exist.")
                continue

            price = Decimal(stock_info[2])
            earnings = price * Decimal(amount)
            tax_percentage = get_tax_percentage(amount, earnings)  # Custom function to determine the tax percentage based on quantity and earnings
            fee = earnings * Decimal(tax_percentage)
            total_earnings = earnings - fee

            current_balance = get_user_balance(self.conn, user_id)
            new_balance = current_balance + total_earnings

            try:
                update_user_balance(self.conn, user_id, new_balance)
            except ValueError as e:
                await ctx.send(f"{ctx.author.mention}, an error occurred while updating the user balance. Error: {str(e)}")
                continue

            cursor.execute("""
                UPDATE user_stocks
                SET amount = amount - ?
                WHERE user_id = ? AND symbol = ?
            """, (amount, user_id, stock_name))

            cursor.execute("""
                UPDATE stocks
                SET available = available + ?
                WHERE symbol = ?
            """, (amount, stock_name))
            decay_other_stocks(self.conn, stock_name)

            # Check if the user is part of a trading team
            cursor.execute("SELECT team_id FROM team_members WHERE user_id=?", (user_id,))
            team_record = cursor.fetchone()
            if team_record:
                team_id = team_record[0]
                # Record the team's transaction
                record_team_transaction(self.conn, team_id, stock_name, amount, price, "sell")
                # Calculate and update the team's profit/loss
                calculate_team_profit_loss(self.conn, team_id)

            await self.decrease_price(ctx, stock_name, amount)
            await log_transaction(ledger_conn, ctx, "Sell Stock", stock_name, amount, earnings, total_earnings, current_balance, new_balance, price)
            last_sell_time[user_id] = current_time
            self.conn.commit()

# Limit Order

    @commands.command(name="limit_order", help="Place a limit order to buy or sell stocks.")
    @is_allowed_user(930513222820331590, PBot)
    async def limit_order(self, ctx, order_type: str, symbol: str, price: float, quantity: int):
        user_id = ctx.author.id

        # Check if the symbol is valid
        cursor = self.conn.cursor()
        cursor.execute("SELECT symbol FROM stocks WHERE symbol=?", (symbol,))
        stock = cursor.fetchone()
        if not stock:
            await ctx.send(f"{ctx.author.mention}, the stock symbol '{symbol}' is not valid.")
            return

        # Check if the order type is valid
        if order_type.lower() not in ["buy", "sell"]:
            await ctx.send(f"{ctx.author.mention}, the order type must be 'buy' or 'sell'.")
            return

        # Check if the user has sufficient balance for a sell order
        if order_type.lower() == "sell":
            cursor.execute("SELECT balance FROM users WHERE user_id=?", (user_id,))
            result = cursor.fetchone()
            if not result or float(result["balance"]) < price * quantity:
                await ctx.send(f"{ctx.author.mention}, you do not have sufficient balance to place this sell order.")
                return

        # Insert the limit order into the database
        try:
            cursor.execute("""
                INSERT INTO limit_orders (user_id, symbol, order_type, price, quantity)
                VALUES (?, ?, ?, ?, ?)
            """, (user_id, symbol, order_type.lower(), price, quantity))
            self.conn.commit()
            await ctx.send(f"{ctx.author.mention}, your limit order has been placed.")
        except sqlite3.Error as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while placing the limit order. Error: {str(e)}")


# Stock User Tools
    @commands.command(name="my_stocks", help="Shows the user's stocks.")
    async def my_stocks(self, ctx):
        user_id = ctx.author.id
        await ctx.message.delete()

        cursor = self.conn.cursor()
        cursor.execute("SELECT symbol, amount FROM user_stocks WHERE user_id=? AND amount > 0", (user_id,))
        user_stocks = cursor.fetchall()

        if not user_stocks:
            await ctx.send(f"{ctx.author.mention}, you do not own any stocks.")
            return

        page_size = 25  # Number of stocks to display per page
        total_pages = (len(user_stocks) + page_size - 1) // page_size

        embeds = []  # List to store the embeds

        for page in range(total_pages):
            embed = discord.Embed(
                title="My Stocks",
                description=f"Stocks owned by {ctx.author.name} (Page {page + 1}/{total_pages}):",
                color=discord.Color.green()
            )

            # Calculate the range of stocks for this page
            start_idx = page * page_size
            end_idx = (page + 1) * page_size

            # Add stocks to the embed for this page
            for stock in user_stocks[start_idx:end_idx]:
                embed.add_field(name=stock['symbol'], value=f"Amount: {stock['amount']}", inline=True)

            embeds.append(embed)

        # Send the first page
        current_page = 0
        message = await ctx.send(embed=embeds[current_page])

        # Add reactions for pagination
        if total_pages > 1:
            await message.add_reaction("⬅️")
            await message.add_reaction("➡️")

            def check(reaction, user):
                return user == ctx.author and str(reaction.emoji) in ["⬅️", "➡️"]

            while True:
                try:
                    reaction, user = await self.bot.wait_for("reaction_add", timeout=60.0, check=check)
                except asyncio.TimeoutError:
                    break

                if str(reaction.emoji) == "➡️" and current_page < total_pages - 1:
                    current_page += 1
                    await message.edit(embed=embeds[current_page])
                elif str(reaction.emoji) == "⬅️" and current_page > 0:
                    current_page -= 1
                    await message.edit(embed=embeds[current_page])

        # Remove reactions after pagination or if there's only one page
        await message.clear_reactions()

    @commands.command(name='list_stocks', aliases=["stocks"])
    async def list_stocks(self, ctx):
        await ctx.message.delete()
        cursor = self.conn.cursor()
        cursor.execute("SELECT symbol, available, price FROM stocks")
        stocks = cursor.fetchall()

        items_per_page = 10  # Number of stocks to display per page
        total_pages = (len(stocks) + items_per_page - 1) // items_per_page

        embed = discord.Embed(title="P3 Stock Market", color=discord.Color.blue())
        embed.set_footer(text="Page 1 of {}".format(total_pages))

        page = 1
        start_index = (page - 1) * items_per_page
        end_index = start_index + items_per_page

        for stock in stocks[start_index:end_index]:
            stock_info = f"{stock[0]}: {stock[1]} (Price: {stock[2]:.6f})"
            embed.add_field(name="Stock", value=stock_info, inline=False)

        message = await ctx.send(embed=embed)

        if total_pages > 1:
            await message.add_reaction("⬅️")
            await message.add_reaction("➡️")

            def check(reaction, user):
                return user == ctx.author and str(reaction.emoji) in ["⬅️", "➡️"]

            while True:
                try:
                    reaction, user = await self.bot.wait_for("reaction_add", timeout=60.0, check=check)
                except TimeoutError:
                    break

                if str(reaction.emoji) == "⬅️" and page > 1:
                    page -= 1
                elif str(reaction.emoji) == "➡️" and page < total_pages:
                    page += 1
                else:
                    continue

                start_index = (page - 1) * items_per_page
                end_index = start_index + items_per_page

                embed.clear_fields()

                for stock in stocks[start_index:end_index]:
                    stock_info = f"{stock[0]}: {stock[1]} (Price: {stock[2]:.6f})"
                    embed.add_field(name="Stock", value=stock_info, inline=False)

                embed.set_footer(text="Page {} of {}".format(page, total_pages))
                await message.edit(embed=embed)
                await message.remove_reaction(reaction, user)

        # Remove reactions at the end
        await message.clear_reactions()

# Stock Tools

    @commands.command(name='topstocks', help='Shows the top 5 highest and lowest price stocks with available quantities.')
    async def topstocks(self, ctx):
        cursor = self.conn.cursor()

        try:
            # Get the top 5 highest price stocks with available quantities
            cursor.execute("SELECT symbol, price, available FROM stocks ORDER BY price DESC LIMIT 5")
            top_high_stocks = cursor.fetchall()

            # Get the top 5 lowest price stocks with available quantities
            cursor.execute("SELECT symbol, price, available FROM stocks ORDER BY price ASC LIMIT 5")
            top_low_stocks = cursor.fetchall()

            # Create the embed
            embed = discord.Embed(title='Top 5 Highest and Lowest Price Stocks', color=discord.Color.blue())

            # Add fields for the top 5 highest price stocks
            for i, (symbol, price, available) in enumerate(top_high_stocks, start=1):
                embed.add_field(name=f"High #{i}: {symbol}", value=f"Price: {price:,.2f} coins\nAvailable: {available}", inline=False)

            # Add fields for the top 5 lowest price stocks
            for i, (symbol, price, available) in enumerate(top_low_stocks, start=1):
                embed.add_field(name=f"Low #{i}: {symbol}", value=f"Price: {price:,.2f} coins\nAvailable: {available}", inline=False)

            await ctx.send(embed=embed)

        except sqlite3.Error as e:
            # Log error message for debugging
            print(f"Database error: {e}")

            # Inform the user that an error occurred
            await ctx.send(f"An error occurred while retrieving stock data. Please try again later.")

        except Exception as e:
            # Log error message for debugging
            print(f"An unexpected error occurred: {e}")

            # Inform the user that an error occurred
            await ctx.send(f"An unexpected error occurred. Please try again later.")



    @commands.command(name="give_stock", help="Give a user an amount of a stock. Deducts it from the total supply.")
    @is_allowed_user(930513222820331590, 236981978153484298, 1006347962043092992)  # The user must have the 'admin' role to use this command.
    async def give_stock(self, ctx, user: discord.User, symbol: str, amount: int):
        cursor = self.conn.cursor()

        await ctx.message.delete()
        # Check if the stock exists.
        cursor.execute("SELECT symbol, available FROM stocks WHERE symbol=?", (symbol,))
        stock = cursor.fetchone()
        if stock is None:
            await ctx.send(f"No stock with symbol {symbol} found.")
            return

        # Check if there's enough of the stock available.
        if stock['available'] < amount:
            await ctx.send(f"Not enough of {symbol} available.")
            return

        # Deduct the stock from the total supply.
        new_available = stock['available'] - amount
        cursor.execute("UPDATE stocks SET available=? WHERE symbol=?", (new_available, symbol))

        # Update the user's stocks.
        cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (user.id, symbol))
        user_stock = cursor.fetchone()
        if user_stock is None:
            cursor.execute("INSERT INTO user_stocks(user_id, symbol, amount) VALUES(?, ?, ?)", (user.id, symbol, amount))
        else:
            new_amount = user_stock['amount'] + amount
            cursor.execute("UPDATE user_stocks SET amount=? WHERE user_id=? AND symbol=?", (new_amount, user.id, symbol))

        self.conn.commit()

        await ctx.send(f"Gave {amount} of {symbol} to {user.name}.")

    @commands.command(name="send_stock", help="Send a user an amount of a stock from your stash.")
    async def send_stock(self, ctx, user: discord.User, symbol: str, amount: int):
        cursor = self.conn.cursor()

        await ctx.message.delete()

        # Check if the stock exists.
        cursor.execute("SELECT symbol, available FROM stocks WHERE symbol=?", (symbol,))
        stock = cursor.fetchone()
        if stock is None:
            await ctx.send(f"No stock with symbol {symbol} found.")
            return

        # Check if there's enough of the stock available in the user's stash.
        cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (ctx.author.id, symbol))
        user_stock = cursor.fetchone()
        if user_stock is None or user_stock['amount'] < amount:
            await ctx.send(f"Not enough of {symbol} available in your stash.")
            return

        # Deduct the stock from the user's stash.
        new_amount = user_stock['amount'] - amount
        cursor.execute("UPDATE user_stocks SET amount=? WHERE user_id=? AND symbol=?", (new_amount, ctx.author.id, symbol))

        # Update the recipient's stocks.
        cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (user.id, symbol))
        recipient_stock = cursor.fetchone()
        if recipient_stock is None:
            cursor.execute("INSERT INTO user_stocks(user_id, symbol, amount) VALUES(?, ?, ?)", (user.id, symbol, amount))
        else:
            new_amount = recipient_stock['amount'] + amount
            cursor.execute("UPDATE user_stocks SET amount=? WHERE user_id=? AND symbol=?", (new_amount, user.id, symbol))

        self.conn.commit()

        # Log the stock transfer
        await log_stock_transfer(ledger_conn, ctx, ctx.author, user, symbol, amount)

        await ctx.send(f"Sent {amount} of {symbol} to {user.name}.")


    @commands.command(name="add_stock", help="Add a new stock. Provide the stock symbol, name, price, total supply, and available amount.")
    @is_allowed_user(930513222820331590, PBot)
    async def add_stock(self, ctx, symbol: str, name: str, price: int, total_supply: int, available: int):
        cursor = self.conn.cursor()
        cursor.execute("INSERT INTO stocks (symbol, name, price, total_supply, available) VALUES (?, ?, ?, ?, ?)",
                       (symbol, name, price, total_supply, available))
        self.conn.commit()
        await ctx.send(f"Added new stock: {symbol} ({name}), price: {price}, total supply: {total_supply}, available: {available}")


    @commands.command(name="add_stock_supply", help="Add to the total supply and available amount of a specified stock.")
    @is_allowed_user(930513222820331590, PBot)
    async def add_stock_supply(self, ctx, symbol: str, amount: int):
        cursor = self.conn.cursor()

        # Update the total supply and available amount of the stock
        try:
            cursor.execute("""
                UPDATE stocks
                SET total_supply = total_supply + ?, available = available + ?
                WHERE symbol = ?
            """, (amount, amount, symbol))
            self.conn.commit()
        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while adding to the stock supply. Error: {str(e)}")
            return

        await ctx.send(f"Successfully added {amount} to the total supply and available amount of {symbol}.")



    @commands.command(name='clear_stocks')
    @is_allowed_user(930513222820331590, PBot)  # Only administrators can run this command
    async def clear_stocks(self, ctx):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM stocks")
        self.conn.commit()
        await ctx.send("Stocks table cleared.")


    @commands.command(name='stock_info', aliases=['stock_metric'], help='Show the current price, available supply, and top holders of a stock.')
    async def stock_info(self, ctx, symbol: str):
        try:
            cursor = self.conn.cursor()

            await ctx.message.delete()
            # Get the current price and stock information
            cursor.execute("SELECT available, price FROM stocks WHERE symbol = ?", (symbol,))
            result = cursor.fetchone()
            if result:
                available, price = result
                await ctx.send(f"Current price of {symbol}: {price} coins\nAvailable supply: {available}")
            else:
                await ctx.send(f"{symbol} is not a valid stock symbol.")

            # Get the top holders of the stock
            cursor.execute("""
                SELECT user_id, amount
                FROM user_stocks
                WHERE symbol = ?
                ORDER BY amount DESC
                LIMIT 10
            """, (symbol,))
            top_holders = cursor.fetchall()

            if top_holders:
                top_holders_str = "\n".join([f"<@{user_id}> - {amount} shares" for user_id, amount in top_holders])
                await ctx.send(f"Top holders of {symbol}:\n{top_holders_str}")
            else:
                await ctx.send(f"No one currently holds {symbol} shares.")

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while fetching stock information: {e}")



# Stock Engine

    @commands.command(name="change_prices")
    @is_allowed_server(P3, SludgeSliders, OM3, PBL)
    @is_allowed_user(930513222820331590, PBot)
    async def change_prices(self, ctx):
        cursor = self.conn.cursor()
        cursor.execute("SELECT symbol, price FROM stocks")
        stocks = cursor.fetchall()

        if not stocks:
            await ctx.send("No stocks found.")
            return

        for stock in stocks:
            symbol = stock[0]
            price = stock[1]
            random_percentage = random.uniform(1, 10)
            change = price * (random_percentage / 100)
            new_price = price + change

            cursor.execute("UPDATE stocks SET price = ? WHERE symbol = ?", (new_price, symbol))

        self.conn.commit()

        await ctx.send("The prices of all stocks have been changed by random percentages.")


    @commands.command(name="incr_price")
    @is_allowed_user(930513222820331590, PBot)
    async def incr_price(self, ctx, percentage: float):
        if percentage <= 0:
            await ctx.send("Invalid percentage. Please provide a positive value.")
            return

        cursor = self.conn.cursor()
        cursor.execute("SELECT symbol, price FROM stocks")
        stocks = cursor.fetchall()

        if not stocks:
            await ctx.send("No stocks found.")
            return

        for stock in stocks:
            symbol = stock[0]
            price = stock[1]
            new_price = price * (1 + (percentage / 100))

            cursor.execute("UPDATE stocks SET price = ? WHERE symbol = ?", (new_price, symbol))

        self.conn.commit()

        await ctx.send(f"The prices of all stocks have been increased by {percentage}%.")


    @commands.command(name="deduct_price")
    @is_allowed_user(930513222820331590, PBot)
    async def deduct_price(self, ctx, percentage: float):
        if percentage <= 0 or percentage >= 100:
            await ctx.send("Invalid percentage. Please provide a value between 0 and 100.")
            return

        cursor = self.conn.cursor()
        cursor.execute("SELECT symbol, price FROM stocks")
        stocks = cursor.fetchall()

        if not stocks:
            await ctx.send("No stocks found.")
            return

        for stock in stocks:
            symbol = stock[0]
            price = stock[1]
            new_price = price * (1 - (percentage / 100))

            cursor.execute("UPDATE stocks SET price = ? WHERE symbol = ?", (new_price, symbol))

        self.conn.commit()

        await ctx.send(f"The prices of all stocks have been deducted by {percentage}%.")


    @commands.command(name="increase_price")
    @is_allowed_server(P3, SludgeSliders, OM3, PBL)
    @is_allowed_user(930513222820331590, PBot)
    async def increase_price(self, ctx, stock_name: str, amount: float):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
        stock = cursor.fetchone()
        if stock is None:
            await ctx.send(f"This stock does not exist.")
            return

        # Generate a random float between 0.000001 * amount and 0.000035 * amount for price increase
        price_increase = random.uniform(buyPressureMin * amount, min(buyPressureMax * amount, 1000000 - float(stock[2])))

        # Assuming `price` is the fourth column
        # Adjust the index if needed
        current_price = float(stock[2])
        new_price = min(current_price + price_increase, stockMax)  # Ensure the price doesn't go above 150,000

        # Update the stock price
        cursor.execute("""
            UPDATE stocks
            SET price = ?
            WHERE symbol = ?
        """, (new_price, stock_name))

        self.conn.commit()

        await ctx.send(f"The price of {stock_name} has increased to {new_price}.")

    @commands.command(name="decrease_price")
    @is_allowed_server(P3, SludgeSliders, OM3, PBL)
    @is_allowed_user(930513222820331590, PBot)
    async def decrease_price(self, ctx, stock_name: str, amount: float):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
        stock = cursor.fetchone()
        if stock is None:
            await ctx.send(f"This stock does not exist.")
            return

        # Generate a random float between 0.000011 * amount and 0.00005 * amount for price decrease
        price_decrease = random.uniform(sellPressureMin * amount, min(sellPressureMax * amount, float(stock[2])))

        # Assuming `price` is the fourth column
        # Adjust the index if needed
        current_price = float(stock[2])
        new_price = max(current_price - price_decrease, stockMin)  # Ensure the price doesn't go below 0.00001

        # Update the stock price
        cursor.execute("""
            UPDATE stocks
            SET price = ?
            WHERE symbol = ?
        """, (float(new_price), stock_name))

        self.conn.commit()

        await ctx.send(f"The price of {stock_name} has decreased to {new_price:.8f}.")


#ETF Tools and Engine

    @commands.command(name="create_etf", help="Create a new ETF.")
    @is_allowed_user(930513222820331590, PBot)
    async def create_etf(self, ctx, name: str, description: str = ""):
        cursor = self.conn.cursor()

        try:
            cursor.execute("INSERT INTO etfs (name, description) VALUES (?, ?)", (name, description))
            self.conn.commit()
            await ctx.send(f"ETF '{name}' created successfully!")
        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while creating the ETF: {str(e)}")

    @commands.command(name="add_etf", help="Add a stock to an ETF.")
    @is_allowed_user(930513222820331590, PBot)
    async def add_etf(self, ctx, etf_id: int, symbol: str, quantity: int):
        cursor = self.conn.cursor()

        try:
            cursor.execute("INSERT INTO etf_stocks (etf_id, symbol, quantity) VALUES (?, ?, ?)", (etf_id, symbol, quantity))
            self.conn.commit()
            await ctx.send(f"Stock '{symbol}' added to ETF {etf_id} successfully!")
        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while adding the stock to the ETF: {str(e)}")

    @commands.command(name="list_etfs", aliases=["etfs"], help="List all ETFs and their values.")
    async def list_etfs(self, ctx):
        cursor = self.conn.cursor()
        cursor.execute("SELECT etf_id, name FROM etfs")
        etfs = cursor.fetchall()

        if not etfs:
            await ctx.send("No ETFs found.")
            return

        embed = discord.Embed(title="ETFs", description="List of all ETFs and their values:", color=discord.Color.blue())

        for etf in etfs:
            etf_id = etf[0]
            etf_name = etf[1]

            # Get stocks associated with the ETF
            cursor.execute("SELECT symbol, quantity FROM etf_stocks WHERE etf_id=?", (etf_id,))
            stocks = cursor.fetchall()

            # Calculate the value of the ETF
            etf_value = 0
            for stock in stocks:
                symbol = stock[0]
                quantity = stock[1]
                cursor.execute("SELECT price FROM stocks WHERE symbol=?", (symbol,))
                stock_price = cursor.fetchone()
                if stock_price:
                    etf_value += stock_price[0] * quantity

            embed.add_field(name=f"ETF ID: {etf_id}", value=f"Name: {etf_name}\nValue: {etf_value:,.2f} coins", inline=False)

        await ctx.message.delete()
        await ctx.send(embed=embed)

    @commands.command(name="my_etfs", help="Show the ETFs owned by the user and their quantities.")
    async def my_etfs(self, ctx):
        user_id = ctx.author.id
        cursor = self.conn.cursor()

        await ctx.message.delete()
        cursor.execute("SELECT etf_id, quantity FROM user_etfs WHERE user_id=?", (user_id,))
        user_etfs = cursor.fetchall()

        if not user_etfs:
            await ctx.send("You don't own any ETFs.")
            return

        total_value = 0
        embed = discord.Embed(title="My ETFs", description="List of ETFs owned by you:", color=discord.Color.blue())

        for etf in user_etfs:
            etf_id = etf[0]
            quantity = etf[1]

            cursor.execute("SELECT name FROM etfs WHERE etf_id=?", (etf_id,))
            etf_name = cursor.fetchone()[0]

            cursor.execute("SELECT SUM(stocks.price * etf_stocks.quantity) FROM etf_stocks JOIN stocks ON etf_stocks.symbol = stocks.symbol WHERE etf_stocks.etf_id=? GROUP BY etf_stocks.etf_id", (etf_id,))
            etf_value = cursor.fetchone()[0]

            total_value += (etf_value or 0) * quantity

            embed.add_field(name=f"ETF ID: {etf_id}", value=f"Name: {etf_name}\nQuantity: {quantity}\nValue: {(etf_value or 0) * quantity:,.2f} coins", inline=False)

        embed.set_footer(text=f"Total Value: {total_value:,.2f} coins")
        await ctx.send(embed=embed)

    @commands.command(name="remove_etf", help="Remove an ETF by its ID.")
    @is_allowed_user(930513222820331590, PBot)
    async def remove_etf(self, ctx, etf_id: int):
        cursor = self.conn.cursor()

        try:
            # Get the ETF's symbol
            cursor.execute("SELECT symbol FROM etf_stocks WHERE etf_id=?", (etf_id,))
            etf_symbol_row = cursor.fetchone()

            if etf_symbol_row:
                etf_symbol = etf_symbol_row[0]

                try:
                    # Remove the ETF from user's holdings
                    cursor.execute("DELETE FROM user_etfs WHERE user_id=? AND etf_id=?", (ctx.author.id, etf_id))

                    try:
                        # Remove ETF from ETF stocks
                        cursor.execute("DELETE FROM etf_stocks WHERE etf_id=?", (etf_id,))

                        self.conn.commit()
                        await ctx.send(f"ETF with ID {etf_id} has been removed from your holdings.")
                    except sqlite3.Error as e:
                        await ctx.send(f"An error occurred while removing the ETF from ETF stocks: {str(e)}")
                except sqlite3.Error as e:
                    await ctx.send(f"An error occurred while removing the ETF from user's holdings: {str(e)}")
            else:
                await ctx.send("No ETF found with the provided ID.")

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while retrieving ETF information: {str(e)}")


    @commands.command(name="remove_etf_from_user", help="Remove an ETF from a specified user's holdings.")
    @is_allowed_user(930513222820331590, PBot)
    async def remove_etf_from_user(self, ctx, user_id: int, etf_id: int):
        cursor = self.conn.cursor()

        try:
            # Remove the ETF from user's holdings
            cursor.execute("DELETE FROM user_etfs WHERE user_id=? AND etf_id=?", (user_id, etf_id))
            self.conn.commit()

            await ctx.send(f"ETF with ID {etf_id} has been removed from user with ID {user_id}'s holdings.")

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while removing the ETF from user's holdings: {str(e)}")



    @commands.command(name="clear_etfs", help="Clear all ETFs from the database.")
    @is_allowed_user(930513222820331590, PBot)
    async def clear_etfs(self, ctx):
        cursor = self.conn.cursor()

        try:
            cursor.execute("DELETE FROM etfs")
            cursor.execute("DELETE FROM etf_stocks")
            cursor.execute("DELETE FROM user_etfs")
            self.conn.commit()
            await ctx.send("All ETFs have been cleared from the database.")
        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while clearing the ETFs: {str(e)}")


    @commands.command(name="debug_etf_value", help="Debug command to find the value of an ETF.")
    async def debug_etf_value(self, ctx, etf_id: int):
        cursor = self.conn.cursor()

        # Retrieve the value of the ETF
        cursor.execute("SELECT value FROM etfs WHERE etf_id=?", (etf_id,))
        result = cursor.fetchone()

        if result is None:
            await ctx.send("ETF not found in the database.")
            return

        etf_value = result[0]

        await ctx.send(f"Debug: Value of ETF {etf_id} is {etf_value} coins.")

    @commands.command(name="update_etf_prices", help="Update prices of stocks in an ETF by a specified percentage.")
    @is_allowed_user(930513222820331590, PBot)
    async def update_etf_prices(self, ctx, etf_id: int, percentage: float):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT symbol, price FROM stocks WHERE symbol IN (
                SELECT symbol FROM etf_stocks WHERE etf_id = ?
            )
        """, (etf_id,))
        stocks = cursor.fetchall()

        if not stocks:
            await ctx.send("The specified ETF does not exist or has no stocks.")
            return

        for stock in stocks:
            symbol = stock[0]
            price = stock[1]

            # Calculate the new price based on the percentage
            new_price = price * (1 + (percentage / 100))

            try:
                cursor.execute("""
                    UPDATE stocks
                    SET price = ?
                    WHERE symbol = ?
                """, (new_price, symbol))
            except sqlite3.Error as e:
                await ctx.send(f"An error occurred while updating the prices. Error: {str(e)}")
                return

        self.conn.commit()

        await ctx.send("Prices of stocks in the ETF have been updated successfully.")

    @commands.command(name="set_etf_prices", help="Set prices of stocks in an ETF to a specified price.")
    @is_allowed_user(930513222820331590, PBot)
    async def set_etf_prices(self, ctx, etf_id: int, price: float):
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE stocks
            SET price = ?
            WHERE symbol IN (
                SELECT symbol FROM etf_stocks WHERE etf_id = ?
            )
        """, (price, etf_id))

        self.conn.commit()

        await ctx.send("Prices of stocks in the ETF have been updated successfully.")


    @commands.command(name="etf_info", help="Display information about a specific ETF.")
    async def etf_info(self, ctx, etf_id: int):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT s.symbol, s.price, s.available
            FROM stocks AS s
            INNER JOIN etf_stocks AS e ON s.symbol = e.symbol
            WHERE e.etf_id = ?
        """, (etf_id,))
        stock_data = cursor.fetchall()

        if not stock_data:
            await ctx.send(f"No stocks found for ETF ID: {etf_id}.")
            return

        etf_price = sum(price for _, price, _ in stock_data)

        # Chunk the stock data into groups of 10 for easier display
        chunk_size = 10
        stock_chunks = list(chunk_list(stock_data, chunk_size))

        # Create an embed for each chunk of stocks
        embeds = []
        for i, stock_chunk in enumerate(stock_chunks, start=1):
            embed = discord.Embed(title=f"ETF Information (ETF ID: {etf_id})", color=discord.Color.blue())
            embed.add_field(name="Stock", value="Price (Coins) - Available Supply", inline=False)
            for stock in stock_chunk:
                symbol = stock[0]
                price = stock[1]
                available = stock[2]
                embed.add_field(name=symbol, value=f"${price:.2f} - {available} available", inline=False)
            if len(stock_chunks) > 1:
                embed.set_footer(text=f"Page {i}/{len(stock_chunks)}")
            embeds.append(embed)

        # Calculate the total ETF value
        total_etf_value = etf_price

        # Add a field to the last embed with the total ETF value
        embeds[-1].add_field(name="Total ETF Value", value=f"${total_etf_value:.2f}", inline=False)

        # Send the first embed and set up pagination if needed
        message = await ctx.send(embed=embeds[0])
        if len(embeds) > 1:
            current_page = 0
            await message.add_reaction("◀️")
            await message.add_reaction("▶️")

            def check(reaction, user):
                return user == ctx.author and str(reaction.emoji) in ["◀️", "▶️"]

            while True:
                try:
                    reaction, user = await self.bot.wait_for("reaction_add", timeout=60.0, check=check)
                except TimeoutError:
                    break

                if str(reaction.emoji) == "▶️" and current_page < len(embeds) - 1:
                    current_page += 1
                elif str(reaction.emoji) == "◀️" and current_page > 0:
                    current_page -= 1

                await message.edit(embed=embeds[current_page])
                await message.remove_reaction(reaction, user)

        await ctx.message.delete()

    async def send_percentage_change_embed(self, ctx, interval, percentage_change):
        # Create an embed for the percentage change message
        embed = discord.Embed(
            title=f"{interval} Percentage Change",
            description=f"{percentage_change:.2f}%",
            color=discord.Color.green()
        )

        # Send the embed message to the channel
        await ctx.send(embed=embed)

    @commands.command(name="update_etf_value", help="Update the name of the ETF 6 voice channel with its value.")
    async def update_etf_value(self, ctx):
        # Explicitly set the locale to use commas as the thousands separator
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

        while True:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT s.price
                FROM stocks AS s
                INNER JOIN etf_stocks AS e ON s.symbol = e.symbol
                WHERE e.etf_id = 6
            """)
            stock_prices = cursor.fetchall()

            if not stock_prices:
                await ctx.send("No stocks found for ETF 6.")
                return

            # Calculate the total value of ETF 6 by summing up the stock prices
            etf_6_value = sum(price for price, in stock_prices)

            # Format the ETF value with commas
            etf_6_value_formatted = locale.format_string("%0.2f", etf_6_value, grouping=True)

            # Find the voice channel by its ID
            voice_channel_id = 1136048044119429160  # Replace with the actual ID
            voice_channel = ctx.guild.get_channel(voice_channel_id)

            if voice_channel:
                # Get the old price from the channel name using regular expressions
                old_name = voice_channel.name
                old_price_match = re.search(r"Market ([\d,]+\.\d+)", old_name)

                if old_price_match:
                    old_price_str = old_price_match.group(1).replace(",", "")
                    try:
                        old_price = float(old_price_str)
                    except ValueError:
                        old_price = None
                else:
                    old_price = None

                if old_price is not None:
                    # Calculate the percentage change
                    percentage_change = ((etf_6_value - old_price) / old_price) * 100

                    # Create an embed for the message
                    embed = discord.Embed(
                        title="Market Value Update",
                        color=discord.Color.green()
                    )
                    embed.add_field(
                        name="Old Price",
                        value=f"${old_price:.2f}",
                        inline=True
                    )
                    embed.add_field(
                        name="New Price",
                        value=f"${etf_6_value:.2f}",
                        inline=True
                    )
                    embed.add_field(
                        name="Percentage Change",
                        value=f"{percentage_change:.2f}%",
                        inline=False
                    )

                    # Update the name of the voice channel with the calculated ETF 6 value
                    new_name = f"Market {etf_6_value_formatted}"
                    await voice_channel.edit(name=new_name)

                    # Send the embed message
                    await ctx.send(embed=embed)
                else:
                    await ctx.send("Failed to retrieve the old price from the channel name.")
            else:
                await ctx.send(f"Voice channel with ID '{voice_channel_id}' not found.")

            # Store the current ETF value in the dictionary
            StockBot.etf_values["5 minutes"] = StockBot.etf_values["15 minutes"]
            StockBot.etf_values["15 minutes"] = StockBot.etf_values["30 minutes"]
            StockBot.etf_values["30 minutes"] = StockBot.etf_values["1 hour"]
            StockBot.etf_values["1 hour"] = StockBot.etf_values["3 hours"]
            StockBot.etf_values["3 hours"] = StockBot.etf_values["6 hours"]
            StockBot.etf_values["6 hours"] = StockBot.etf_values["12 hours"]
            StockBot.etf_values["12 hours"] = StockBot.etf_values["24 hours"]
            StockBot.etf_values["24 hours"] = etf_6_value

            # Calculate and print percentage changes for different time intervals
            current_time = time.time()
            for interval, old_value in StockBot.etf_values.items():
                if old_value is not None:
                    elapsed_time = current_time - (60 * int(interval.split()[0]))
                    if elapsed_time > 0:
                        percentage_change = ((etf_6_value - old_value) / old_value) * 100
                        await self.send_percentage_change_embed(ctx, interval, percentage_change)

            # Wait for 120 seconds before checking again
            await asyncio.sleep(120)



# Buy/Sell ETFs
    @commands.command(name="buy_etf", help="Buy an ETF. Provide the ETF ID and quantity.")
    @is_allowed_server(P3, SludgeSliders, OM3, PBL)
    async def buy_etf(self, ctx, etf_id: int, quantity: int):
        user_id = ctx.author.id
        cursor = self.conn.cursor()

        await ctx.message.delete()
        # Check if user already holds the maximum allowed quantity of the ETF
        cursor.execute("SELECT COUNT(*) FROM user_etfs WHERE user_id=? AND etf_id=?", (user_id, etf_id))
        current_holding = cursor.fetchone()[0]

        # Fetch the ETF's value and calculate the total cost
        cursor.execute("SELECT SUM(stocks.price * etf_stocks.quantity) FROM etf_stocks JOIN stocks ON etf_stocks.symbol = stocks.symbol WHERE etf_stocks.etf_id=?", (etf_id,))
        etf_value = cursor.fetchone()[0]

        if etf_value is None:
            await ctx.send("Invalid ETF ID.")
            return

        total_cost = Decimal(etf_value) * Decimal(quantity)


        # Calculate the tax amount based on dynamic factors
        tax_percentage = get_tax_percentage(quantity, total_cost)  # Custom function to determine the tax percentage based on quantity and total_cost
        fee = total_cost * Decimal(tax_percentage)
        total_cost_with_tax = total_cost + fee

        # Check if user has enough balance to buy the ETF
        current_balance = get_user_balance(self.conn, user_id)

        if total_cost_with_tax > current_balance:
            # Calculate the missing amount needed to complete the transaction including tax.
            missing_amount = total_cost - current_balance
            await ctx.send(f"{ctx.author.mention}, you do not have enough coins to buy this etf. You need {missing_amount:.2f} more coins, including tax, to complete this purchase.")
            return

        new_balance = current_balance - total_cost_with_tax

        try:
            update_user_balance(self.conn, user_id, new_balance)
        except ValueError as e:
            await ctx.send(f"An error occurred while updating the user balance. Error: {str(e)}")
            return

        # Update user's ETF holdings
        try:
            cursor.execute("""
                INSERT INTO user_etfs (user_id, etf_id, quantity)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id, etf_id) DO UPDATE SET quantity = quantity + ?
            """, (user_id, etf_id, quantity, quantity))
        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while updating user ETF holdings. Error: {str(e)}")
            return

        decay_other_stocks(self.conn, "P3:BANK")
        await log_transaction(ledger_conn, ctx, "Buy ETF", etf_id, quantity, total_cost, total_cost_with_tax, current_balance, new_balance, etf_value)
        self.conn.commit()

        await ctx.send(f"You have successfully bought {quantity} units of ETF {etf_id}. Your new balance is: {new_balance} coins.")


    @commands.command(name="sell_etf", help="Sell an ETF. Provide the ETF ID and quantity.")
    async def sell_etf(self, ctx, etf_id: int, quantity: int):
        user_id = ctx.author.id
        cursor = self.conn.cursor()

        await ctx.message.delete()
        # Check if user holds the specified quantity of the ETF
        cursor.execute("SELECT quantity FROM user_etfs WHERE user_id=? AND etf_id=?", (user_id, etf_id))
        current_holding = cursor.fetchone()

        if current_holding is None or current_holding[0] < quantity:
            await ctx.send("Insufficient ETF holdings.")
            return

        # Fetch the ETF's value and calculate the total sale amount
        cursor.execute("SELECT SUM(stocks.price * etf_stocks.quantity) FROM etf_stocks JOIN stocks ON etf_stocks.symbol = stocks.symbol WHERE etf_stocks.etf_id=?", (etf_id,))
        etf_value = cursor.fetchone()[0]

        if etf_value is None:
            await ctx.send("Invalid ETF ID.")
            return

        total_sale_amount = Decimal(etf_value) * Decimal(quantity)

        # Calculate the tax amount based on dynamic factors
        tax_percentage = get_tax_percentage(quantity, total_sale_amount)  # Custom function to determine the tax percentage based on quantity and total_sale_amount
        fee = total_sale_amount * Decimal(tax_percentage)
        total_sale_amount_with_tax = total_sale_amount - fee

        # Update user's balance
        current_balance = get_user_balance(self.conn, user_id)
        new_balance = current_balance + total_sale_amount_with_tax

        try:
            update_user_balance(self.conn, user_id, new_balance)
        except ValueError as e:
            await ctx.send(f"An error occurred while updating the user balance. Error: {str(e)}")
            return

        # Update user's ETF holdings
        try:
            cursor.execute("""
                UPDATE user_etfs
                SET quantity = quantity - ?
                WHERE user_id = ? AND etf_id = ?
            """, (quantity, user_id, etf_id))
        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while updating user ETF holdings. Error: {str(e)}")
            return
        decay_other_stocks(self.conn, "P3:BANK")
        await log_transaction(ledger_conn, ctx, "Sell ETF", etf_id, quantity, total_sale_amount, total_sale_amount_with_tax, current_balance, new_balance, etf_value)
        self.conn.commit()

        await ctx.send(f"You have successfully sold {quantity} units of ETF {etf_id}. Your new balance is: {new_balance} coins.")

    @commands.command(name="sell_all_etf", help="Sell all units of specific ETFs.")
    async def sell_all_etf(self, ctx, *etf_ids: int):
        user_id = ctx.author.id
        cursor = self.conn.cursor()

        await ctx.message.delete()

        if not etf_ids:
            await ctx.send("You need to specify at least one ETF ID to sell.")
            return

        sold_etfs = []

        for etf_id in etf_ids:
            # Check if the user holds any units of the specified ETF
            cursor.execute("SELECT quantity FROM user_etfs WHERE user_id=? AND etf_id=?", (user_id, etf_id))
            current_holding = cursor.fetchone()

            if current_holding is None or current_holding[0] <= 0:
                await ctx.send(f"You don't have any units of ETF {etf_id} to sell.")
            else:
                quantity = current_holding[0]  # Get the quantity of ETFs sold

                # Fetch the ETF's value and calculate the total sale amount
                cursor.execute("SELECT SUM(stocks.price * etf_stocks.quantity) FROM etf_stocks JOIN stocks ON etf_stocks.symbol = stocks.symbol WHERE etf_stocks.etf_id=?", (etf_id,))
                etf_value = cursor.fetchone()[0]

                if etf_value is None:
                    await ctx.send(f"Invalid ETF ID {etf_id}. Skipping this ETF.")
                else:
                    total_sale_amount = Decimal(etf_value) * Decimal(quantity)

                    # Calculate the tax amount based on dynamic factors
                    tax_percentage = get_tax_percentage(quantity, total_sale_amount)  # Custom function to determine the tax percentage based on quantity and total_sale_amount
                    fee = total_sale_amount * Decimal(tax_percentage)
                    total_sale_amount_with_tax = total_sale_amount - fee

                    # Update user's balance
                    current_balance = get_user_balance(self.conn, user_id)
                    new_balance = current_balance + total_sale_amount_with_tax

                    try:
                        update_user_balance(self.conn, user_id, new_balance)
                    except ValueError as e:
                        await ctx.send(f"An error occurred while updating the user balance. Error: {str(e)}")
                    else:
                        # Remove user's ETF holdings for the specified ETF
                        try:
                            cursor.execute("DELETE FROM user_etfs WHERE user_id=? AND etf_id=?", (user_id, etf_id))
                        except sqlite3.Error as e:
                            await ctx.send(f"An error occurred while removing user ETF holdings. Error: {str(e)}")
                        else:
                            decay_other_stocks(self.conn, "P3:BANK")
                            await log_transaction(ledger_conn, ctx, "Sell ALL ETF", etf_id, quantity, total_sale_amount, total_sale_amount_with_tax, current_balance, new_balance, etf_value)
                            self.conn.commit()
                            sold_etfs.append(etf_id)

        if sold_etfs:
            sold_etf_list = ", ".join(map(str, sold_etfs))
            await ctx.send(f"You have successfully sold all units of ETFs {sold_etf_list}. Your new balance is: {new_balance} coins.")





    @commands.command(name="admin_close_etf", help="Admin command to close all user positions for a specified ETF.")
    @is_allowed_user(930513222820331590, PBot)
    async def admin_close_etf(self, ctx, etf_id: int):
        cursor = self.conn.cursor()

        # Fetch all user IDs and quantities holding the specified ETF
        cursor.execute("SELECT user_id, quantity FROM user_etfs WHERE etf_id=?", (etf_id,))
        user_positions = cursor.fetchall()

        if not user_positions:
            await ctx.send("No user positions found for the specified ETF.")
            return

        # Iterate through user positions and close them
        for user_id, quantity in user_positions:
            # Fetch the ETF's value and calculate the total sale amount for each user
            cursor.execute("SELECT SUM(stocks.price * etf_stocks.quantity) FROM etf_stocks JOIN stocks ON etf_stocks.symbol = stocks.symbol WHERE etf_stocks.etf_id=?", (etf_id,))
            etf_value = cursor.fetchone()[0]

            if etf_value is None:
                await ctx.send(f"Invalid ETF ID for user {user_id}. Skipping.")
                continue

            total_sale_amount = Decimal(etf_value) * Decimal(quantity)

            # Calculate the tax amount based on dynamic factors
            tax_percentage = get_tax_percentage(quantity, total_sale_amount)  # Custom function to determine the tax percentage based on quantity and total_sale_amount
            fee = total_sale_amount * Decimal(tax_percentage)
            total_sale_amount_with_tax = total_sale_amount - fee

            # Update user's balance
            current_balance = get_user_balance(self.conn, user_id)
            new_balance = current_balance + total_sale_amount_with_tax

            try:
                update_user_balance(self.conn, user_id, new_balance)
            except ValueError as e:
                await ctx.send(f"An error occurred while updating the user balance for user {user_id}. Error: {str(e)}")
                continue

        # Remove all user positions for the specified ETF
        try:
            cursor.execute("DELETE FROM user_etfs WHERE etf_id=?", (etf_id,))
        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while removing user ETF positions. Error: {str(e)}")
            return

        decay_other_stocks(self.conn, "P3:BANK")
        self.conn.commit()

        await ctx.send(f"All user positions for ETF {etf_id} have been successfully closed.")


# Economy Tools

## Begin Lottery
    @commands.command(name="buy_tickets", help="Buy raffle tickets.")
    @is_allowed_server(P3, SludgeSliders, OM3, PBL)
    async def buy_tickets(self, ctx, quantity: int):
        await ctx.message.delete()
        if quantity <= 0:
            await ctx.send("The quantity of tickets to buy should be greater than 0.")
            return

        user_id = ctx.author.id
        user_balance = get_user_balance(self.conn, user_id)
        cost = quantity * ticketPrice

        if user_balance < cost:
            await ctx.send(f"{ctx.author.mention}, you don't have enough coins to buy {quantity} tickets. You need {cost} coins.")
            return

        # Calculate the tax amount based on dynamic factors
        tax_percentage = get_tax_percentage(quantity, cost)  # Custom function to determine the tax percentage based on quantity and cost
        fee = cost * Decimal(tax_percentage)
        total_cost = cost + fee

        # Check if the user will have a negative balance after buying the tickets
        if user_balance - total_cost < 0:
            await ctx.send(f"{ctx.author.mention}, this transaction will result in a negative balance. Please buy a lower quantity.")
            return

        # Deduct the cost of tickets from the user's balance
        update_user_balance(self.conn, user_id, user_balance - total_cost)

        ticket_data = await get_ticket_data(self.conn, user_id)

        if ticket_data is None:
            await update_ticket_data(self.conn, user_id, quantity, int(time.time()))
        else:
            ticket_quantity, _ = ticket_data
            await update_ticket_data(self.conn, user_id, ticket_quantity + quantity, int(time.time()))

        await ctx.send(f"{ctx.author.mention}, you have successfully bought {quantity} tickets.")


# Lottery Tools

    @commands.command(name="draw_winner", help="Draw a winner for the raffle.")
    @is_allowed_user(930513222820331590, PBot)
    async def draw_winner(self, ctx):
        cursor = self.conn.cursor()
        cursor.execute("SELECT user_id, quantity FROM raffle_tickets")
        ticket_data = cursor.fetchall()

        if not ticket_data:
            await ctx.send("No tickets were sold. There is no winner.")
            return

        total_tickets = sum(quantity for _, quantity in ticket_data)
        winner_ticket = random.randint(1, total_tickets)
        accumulated_tickets = 0

        for user_id, quantity in ticket_data:
            accumulated_tickets += quantity
            if accumulated_tickets >= winner_ticket:
                winner = await self.bot.fetch_user(user_id)
                if winner:
                    channel = ctx.guild.get_channel(1087743565016346634)
                    # Add a countdown before announcing the winner
                    count = 5
                    message = await ctx.send(f"Drawing winner in {count}...")
                    for i in range(count, 0, -1):
                        await message.edit(content=f"Drawing winner in {i}...")
                        await asyncio.sleep(1)
                    await ctx.send(f"The winner is {winner.mention}! Congratulations! Please head over to {channel.name} to claim your prize.")
                else:
                    await ctx.send(f"Error..Report..{channel.name}")
                break

    @commands.command(name="clear_tickets")
    @is_allowed_user(930513222820331590, PBot)
    async def clear_tickets(self, ctx):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM raffle_tickets")
        self.conn.commit()

        await ctx.send(f"All raffle tickets have been deleted.")

    @commands.command(name="give_tickets")
    @is_allowed_user(930513222820331590, PBot)
    async def give_tickets(self, ctx, user: discord.Member, quantity: int):
        if quantity < 1:
            await ctx.send("Quantity must be at least 1.")
            return

        insert_raffle_tickets(self.conn, user.id, quantity, int(time.time()))

        await ctx.send(f"Gave {quantity} raffle ticket(s) to {user.mention}.")

    @commands.command(name="my_tickets", help="Check your raffle tickets.")
    async def my_tickets(self, ctx):
        user_id = ctx.author.id
        ticket_data = await get_ticket_data(self.conn, user_id)

        if ticket_data is None:
            await ctx.send(f"{ctx.author.mention}, you have no tickets.")
        else:
            ticket_quantity, _ = ticket_data
            await ctx.send(f"{ctx.author.mention}, you have {ticket_quantity} tickets.")



## Begin Marketplace
    @commands.command(name="marketplace", help="View the items available in the marketplace.")
    @is_allowed_user(930513222820331590, PBot)
    async def view_marketplace(self, ctx):
        cursor = self.conn.cursor()

        # Fetch all items from the marketplace
        cursor.execute("SELECT item_name, item_description, price, is_usable FROM items")
        items_data = cursor.fetchall()

        if not items_data:
            await ctx.send("No items available in the marketplace.")
            return

        # Create and send an embed with the items' information
        embed = discord.Embed(title="Marketplace", color=discord.Color.blue())

        for item in items_data:
            item_name = item[0]
            item_description = item[1] or "No description available"
            item_price = item[2]
            is_usable = item[3]

            embed.add_field(name=item_name, value=f"Description: {item_description}\nPrice: {item_price} coins\nUsable: {'Yes' if is_usable else 'No'}", inline=False)

        await ctx.send(embed=embed)


    @commands.command(name="inventory", help="View your inventory.")
    async def view_inventory(self, ctx):
        user_id = ctx.author.id
        cursor = self.conn.cursor()

        # Fetch user's inventory
        cursor.execute("""
            SELECT items.item_name, items.item_description, inventory.quantity
            FROM inventory
            JOIN items ON inventory.item_id = items.item_id
            WHERE user_id=?
        """, (user_id,))
        inventory_data = cursor.fetchall()

        if not inventory_data:
            await ctx.send(f"{ctx.author.mention}, your inventory is empty.")
            return

        # Create and send an embed with the user's inventory
        embed = discord.Embed(title="Your Inventory", color=discord.Color.green())

        for item in inventory_data:
            item_name = item[0]
            item_description = item[1] or "No description available"
            quantity = item[2]

            embed.add_field(name=item_name, value=f"Description: {item_description}\nQuantity: {quantity}", inline=False)

        await ctx.send(embed=embed)

    @commands.command(name="buy_item", help="Buy an item from the marketplace.")
    @is_allowed_user(930513222820331590, PBot)
    async def buy_item(self, ctx, item_name: str, quantity: int):
        user_id = ctx.author.id
        cursor = self.conn.cursor()

        # Check if the item exists in the marketplace
        cursor.execute("SELECT item_id, price, is_usable FROM items WHERE item_name=?", (item_name,))
        item_data = cursor.fetchone()

        if item_data is None:
            await ctx.send(f"{ctx.author.mention}, this item is not available in the marketplace.")
            return

        item_id, item_price, is_usable = item_data

        # Calculate the total cost
        total_cost = Decimal(item_price) * Decimal(quantity)

        # Calculate the tax amount based on dynamic factors
        tax_percentage = get_tax_percentage(quantity, total_cost)  # Custom function to determine the tax percentage based on quantity and cost
        tax_amount = Decimal(total_cost) * Decimal(tax_percentage)

        # Check if the user has enough balance to buy the items
        current_balance = get_user_balance(self.conn, user_id)

        if total_cost + tax_amount > current_balance:
            await ctx.send(f"{ctx.author.mention}, you do not have enough coins to buy these items.")
            return

        new_balance = current_balance - (total_cost + tax_amount)

        # Update the user's balance
        try:
            update_user_balance(self.conn, user_id, new_balance)
        except ValueError as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while updating the user balance. Error: {str(e)}")
            return

        # Update the user's inventory
        try:
            cursor.execute("""
                INSERT INTO inventory (user_id, item_id, quantity)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id, item_id) DO UPDATE SET quantity = quantity + ?
            """, (user_id, item_id, quantity, quantity))
        except sqlite3.Error as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while updating user inventory. Error: {str(e)}")
            return

        # Record the transaction
        try:
            cursor.execute("""
                INSERT INTO transactions (user_id, item_id, quantity, total_cost, tax_amount)
                VALUES (?, ?, ?, ?, ?)
            """, (user_id, item_id, quantity, total_cost, tax_amount))
        except sqlite3.Error as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while recording the transaction. Error: {str(e)}")
            return

        self.conn.commit()

        await ctx.send(f"{ctx.author.mention}, you have successfully bought {quantity} {item_name}. Your new balance is: {new_balance} coins.")


    @commands.command(name="sell_item", help="Sell an item from your inventory.")
    @is_allowed_user(930513222820331590, PBot)
    async def sell_item(self, ctx, item_name: str, quantity: int):
        user_id = ctx.author.id
        cursor = self.conn.cursor()

        # Check if the item exists in the marketplace
        cursor.execute("SELECT item_id, price FROM items WHERE item_name=?", (item_name,))
        item_data = cursor.fetchone()

        if item_data is None:
            await ctx.send(f"{ctx.author.mention}, this item is not available in the marketplace.")
            return

        item_id, item_price = item_data

        # Check if the user has enough quantity of the item to sell
        cursor.execute("SELECT quantity FROM inventory WHERE user_id=? AND item_id=?", (user_id, item_id))
        current_holding = cursor.fetchone()

        if current_holding is None or current_holding[0] < quantity:
            await ctx.send(f"{ctx.author.mention}, you do not have enough {item_name} in your inventory to sell.")
            return

        # Calculate the total sale amount
        total_sale_amount = Decimal(item_price) * Decimal(quantity)

        # Update user's balance
        current_balance = get_user_balance(self.conn, user_id)
        new_balance = current_balance + total_sale_amount

        try:
            update_user_balance(self.conn, user_id, new_balance)
        except ValueError as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while updating the user balance. Error: {str(e)}")
            return

        # Update user's inventory
        try:
            cursor.execute("UPDATE inventory SET quantity = quantity - ? WHERE user_id = ? AND item_id = ?", (quantity, user_id, item_id))
        except sqlite3.Error as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while updating user inventory. Error: {str(e)}")
            return

        # Record the transaction
        try:
            cursor.execute("""
                INSERT INTO transactions (user_id, item_id, quantity, total_cost, tax_amount)
                VALUES (?, ?, ?, ?, ?)
            """, (user_id, item_id, quantity, total_sale_amount, Decimal(0)))
        except sqlite3.Error as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while recording the transaction. Error: {str(e)}")
            return

        self.conn.commit()

        await ctx.send(f"{ctx.author.mention}, you have successfully sold {quantity} {item_name}. Your new balance is: {new_balance} coins.")


## Market Place Tools

    @commands.command(name="add_item", help="Add an item to the marketplace.")
    @is_allowed_user(930513222820331590, PBot)
    async def add_item(self, ctx, item_name: str, price: float, item_description: str = "", is_usable: bool = False):
        conn = sqlite3.connect("currency_system.db")
        cursor = conn.cursor()

        try:
            # Insert the new item into the 'items' table
            cursor.execute("""
                INSERT INTO items (item_name, item_description, price, is_usable)
                VALUES (?, ?, ?, ?)
            """, (item_name, item_description, price, int(is_usable)))

            conn.commit()

            await ctx.send(f"The item '{item_name}' has been added to the marketplace.")
        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while adding the item: {e}")
        finally:
            conn.close()


## End Marketplace

## Games




##

    @commands.command(name="calculate", aliases=["calculate_stock"], help="Calculate the total cost, tax, and final amount for buying stocks. Provide the stock name and amount.")
    async def calculate(self, ctx, stock_name: str, amount: int):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
        stock = cursor.fetchone()
        if stock is None:
            await ctx.send(f"{ctx.author.mention}, this stock does not exist.")
            return

        available, price = int(stock[4]), Decimal(stock[2])

        if amount > available:
            await ctx.send(f"{ctx.author.mention}, there are only {available} {stock_name} stocks available.")
            return

        cost = price * Decimal(amount)

        # Calculate the tax amount based on dynamic factors
        tax_percentage = get_tax_percentage(amount, cost)  # Custom function to determine the tax percentage based on quantity and cost
        tax_amount = cost * Decimal(tax_percentage)
        total_cost = cost + tax_amount

        await ctx.send(
            f"{ctx.author.mention}, here's the breakdown for {amount} {stock_name} stocks:\n"
            f"- Stock Price: {price} coins each\n"
            f"- Subtotal: {cost} coins\n"
            f"- Tax ({tax_percentage * 100}%): {tax_amount} coins\n"
            f"- Total Cost: {total_cost} coins."
        )


    @commands.command(name="adjust_balance", help="Adjust a specific user's balance.")
    @is_allowed_user(930513222820331590, PBot)
    async def adjust_balance(self, ctx, user_id: int, new_balance: decimal.Decimal):

        # Convert decimal.Decimal value to string
        new_balance_str = str(new_balance)

        # Update the user's balance in the database
        cursor = self.conn.cursor()
        try:
            cursor.execute("UPDATE users SET balance = ? WHERE user_id = ?", (new_balance_str, user_id))
            self.conn.commit()
            await ctx.send(f"The balance for user with ID {user_id} has been adjusted to {new_balance:.2f} coins.")
        except Exception as e:
            await ctx.send(f"An error occurred while adjusting the balance: {str(e)}")


    @commands.command(name="fix_negative_balances", help="Fix negative balances for all users.")
    @is_allowed_user(930513222820331590, PBot)
    @is_allowed_server(P3, SludgeSliders, OM3, PBL)  # Add your server IDs here
    async def fix_negative_balances(self, ctx):
        cursor = self.conn.cursor()

        # Get all users with negative balances
        cursor.execute("SELECT user_id, balance FROM users WHERE balance < 0")
        users_with_negative_balance = cursor.fetchall()

        # Reset negative balances to zero and update the database
        for user_id, balance in users_with_negative_balance:
            cursor.execute("UPDATE users SET balance = 0 WHERE user_id = ?", (user_id,))
            self.conn.commit()

        await ctx.send("Negative balances have been fixed for all users.")


## Trading Firm

    @commands.command(name="create_trading_team", help="Create a new trading team. Provide the team name.")
    @is_allowed_user(930513222820331590, PBot)
    async def create_trading_team(self, ctx, team_name: str):
        user_id = ctx.author.id

        # Check if the user is already in a team
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM team_members WHERE user_id=?", (user_id,))
        existing_team = cursor.fetchone()
        if existing_team:
            await ctx.send(f"{ctx.author.mention}, you are already in a team.")
            return

        try:
            cursor.execute("""
                INSERT INTO trading_teams (name)
                VALUES (?)
            """, (team_name,))
            team_id = cursor.lastrowid
            cursor.execute("""
                INSERT INTO team_members (user_id, team_id)
                VALUES (?, ?)
            """, (user_id, team_id))
            self.conn.commit()
            await ctx.send(f"{ctx.author.mention}, you have successfully created the trading team '{team_name}' and joined it!")
        except sqlite3.Error as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while creating the trading team. Error: {str(e)}")
            self.conn.rollback()


    @commands.command(name="join_trading_team", help="Join an existing trading team. Provide the team ID.")
    @is_allowed_user(930513222820331590, PBot)
    async def join_trading_team(self, ctx, team_id: int):
        user_id = ctx.author.id

        # Check if the user is already in a team
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM team_members WHERE user_id=?", (user_id,))
        existing_team = cursor.fetchone()
        if existing_team:
            await ctx.send(f"{ctx.author.mention}, you are already in a team.")
            return

        # Check if the specified team exists
        cursor.execute("SELECT * FROM trading_teams WHERE team_id=?", (team_id,))
        team = cursor.fetchone()
        if team is None:
            await ctx.send(f"{ctx.author.mention}, this team does not exist.")
            return

        try:
            cursor.execute("""
                INSERT INTO team_members (user_id, team_id)
                VALUES (?, ?)
            """, (user_id, team_id))
            self.conn.commit()
            await ctx.send(f"{ctx.author.mention}, you have successfully joined the trading team '{team[1]}'!")
        except sqlite3.Error as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while joining the trading team. Error: {str(e)}")
            self.conn.rollback()

    @commands.command(name="leave_trading_team", help="Leave your current trading team.")
    async def leave_trading_team(self, ctx):
        user_id = ctx.author.id

        # Check if the user is in a team
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM team_members WHERE user_id=?", (user_id,))
        existing_team = cursor.fetchone()
        if not existing_team:
            await ctx.send(f"{ctx.author.mention}, you are not in a team.")
            return

        try:
            cursor.execute("""
                DELETE FROM team_members
                WHERE user_id = ?
            """, (user_id,))
            self.conn.commit()
            await ctx.send(f"{ctx.author.mention}, you have successfully left your trading team!")
        except sqlite3.Error as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while leaving the trading team. Error: {str(e)}")
            self.conn.rollback()


    @commands.command(name="list_trading_teams", aliases=['teams'], help="List all trading teams with P/L metrics and member count.")
    async def list_trading_teams(self, ctx):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT trading_teams.team_id, trading_teams.name, trading_teams.total_profit_loss, team_members.user_id
            FROM trading_teams
            LEFT JOIN team_members ON trading_teams.team_id = team_members.team_id
        """)
        teams = cursor.fetchall()

        if not teams:
            await ctx.send("There are no trading teams.")
            return

        # Create a dictionary to store team information
        team_info = {}

        for team in teams:
            team_id = team[0]
            team_name = team[1]
            total_profit_loss = team[2]
            member_id = team[3]

            # Get Discord username from user ID
            member = ctx.guild.get_member(member_id)
            member_name = member.name if member else f"User ID: {member_id}"

            # Add team information to the dictionary
            if team_id not in team_info:
                team_info[team_id] = {
                    "name": team_name,
                    "profit_loss": total_profit_loss,
                    "members": [member_name],
                }
            else:
                team_info[team_id]["members"].append(member_name)

        # Sort teams by profit in descending order
        sorted_teams = sorted(team_info.items(), key=lambda x: x[1]["profit_loss"], reverse=True)

        embed = discord.Embed(title="Trading Firm Profit/Loss Spread", color=discord.Color.blue())

        # Add each team's information as a field in the embed
        for team_id, info in sorted_teams:
            team_name = info["name"]
            total_profit_loss = info["profit_loss"]
            member_list = ", ".join(info["members"])
            profit_loss_str = f"**P/L:** {format(total_profit_loss, ',')}"

            embed.add_field(name=f"**Team ID: {team_id}** - {team_name}", value=f"{profit_loss_str}\n**Members:** {member_list}", inline=False)

        await ctx.send(embed=embed)




    @commands.command(name="add_member", help="Add a specific member to a specific trading team. [Admin Only]")
    @is_allowed_user(930513222820331590, PBot)
    async def add_member(self, ctx, team_id: int, member: discord.Member):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM trading_teams WHERE team_id=?", (team_id,))
        team = cursor.fetchone()
        if not team:
            await ctx.send(f"Team with ID {team_id} does not exist.")
            return

        try:
            cursor.execute("""
                INSERT INTO team_members (user_id, team_id)
                VALUES (?, ?)
            """, (member.id, team_id))
            self.conn.commit()
            await ctx.send(f"{ctx.author.mention}, {member.mention} has been successfully added to team {team_id}!")
        except sqlite3.Error as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while adding the member to the team. Error: {str(e)}")
            self.conn.rollback()

    @commands.command(name="remove_member", help="Remove a specific member from a specific trading team. [Admin Only]")
    @is_allowed_user(930513222820331590, PBot)
    async def remove_member(self, ctx, team_id: int, member: discord.Member):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM team_members WHERE team_id=? AND user_id=?", (team_id, member.id))
        existing_member = cursor.fetchone()
        if not existing_member:
            await ctx.send(f"{member.mention} is not in team {team_id}.")
            return

        try:
            cursor.execute("""
                DELETE FROM team_members
                WHERE user_id = ? AND team_id = ?
            """, (member.id, team_id))
            self.conn.commit()
            await ctx.send(f"{ctx.author.mention}, {member.mention} has been successfully removed from team {team_id}!")
        except sqlite3.Error as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while removing the member from the team. Error: {str(e)}")
            self.conn.rollback()

    @commands.command(name="clear_teams", help="Clear all trading team database entries. [Admin Only]")
    @is_allowed_user(930513222820331590, PBot)
    async def clear_teams(self, ctx):
        confirmation_message = await ctx.send(f"{ctx.author.mention}, are you sure you want to clear all trading team data? This action is irreversible. Type 'yes' to confirm.")

        def check(m):
            return m.author == ctx.author and m.content.lower() == 'yes'

        try:
            await self.bot.wait_for('message', timeout=60.0, check=check)
        except asyncio.TimeoutError:
            await confirmation_message.edit(content="Clearing trading team data aborted. No confirmation received.")
            return

        cursor = self.conn.cursor()
        try:
            cursor.execute("DELETE FROM team_members")
            cursor.execute("DELETE FROM trading_teams")
            self.conn.commit()
            await ctx.send(f"{ctx.author.mention}, all trading team data has been cleared successfully.")
        except sqlite3.Error as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while clearing trading team data. Error: {str(e)}")
            self.conn.rollback()

    @commands.command(name="set_stock_supply", help="Set the total and available supply for a specific stock.")
    @is_allowed_user(930513222820331590, PBot)
    async def set_stock_supply(self, ctx, stock_name: str, total_supply: typing.Optional[int] = None, available_supply: typing.Optional[int] = None):
        cursor = self.conn.cursor()

        try:
            # Check if the stock exists
            cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
            stock = cursor.fetchone()
            if stock is None:
                await ctx.send(f"This stock '{stock_name}' does not exist.")
                return

            if total_supply is not None:
                if total_supply < 0:
                    await ctx.send("Invalid total supply value. The total supply must be non-negative.")
                    return
                cursor.execute("UPDATE stocks SET total_supply = ? WHERE symbol = ?", (total_supply, stock_name))
                await ctx.send(f"The total supply for {stock_name} has been set to {total_supply}.")

            if available_supply is not None:
                if available_supply < 0:
                    await ctx.send("Invalid available supply value. The available supply must be non-negative.")
                    return
                cursor.execute("UPDATE stocks SET available = ? WHERE symbol = ?", (available_supply, stock_name))
                await ctx.send(f"The available supply for {stock_name} has been set to {available_supply}.")

            self.conn.commit()

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while updating the stock supply. Error: {str(e)}")

    @commands.command(name="set_etf_supply", help="Set the total and available supply for all stocks within a specified ETF.")
    @is_allowed_user(930513222820331590, PBot)
    async def set_etf_supply(self, ctx, etf_id: int, total_supply: typing.Optional[int] = None, available_supply: typing.Optional[int] = None):
        cursor = self.conn.cursor()

        try:
            # Check if the ETF exists
            cursor.execute("SELECT * FROM etfs WHERE etf_id=?", (etf_id,))
            etf = cursor.fetchone()
            if etf is None:
                await ctx.send(f"This ETF with ID {etf_id} does not exist.")
                return

            # Get the list of stocks within the ETF
            cursor.execute("SELECT symbol FROM etf_stocks WHERE etf_id=?", (etf_id,))
            stocks = cursor.fetchall()

            if not stocks:
                await ctx.send(f"No stocks found within ETF ID {etf_id}.")
                return

            if total_supply is not None:
                if total_supply < 0:
                    await ctx.send("Invalid total supply value. The total supply must be non-negative.")
                    return

                # Update total supply for all stocks within the ETF
                for stock in stocks:
                    stock_name = stock[0]
                    cursor.execute("UPDATE stocks SET total_supply = ? WHERE symbol = ?", (total_supply, stock_name))
                await ctx.send(f"The total supply for all stocks within ETF {etf_id} has been set to {total_supply}.")

            if available_supply is not None:
                if available_supply < 0:
                    await ctx.send("Invalid available supply value. The available supply must be non-negative.")
                    return

                # Update available supply for all stocks within the ETF
                for stock in stocks:
                    stock_name = stock[0]
                    cursor.execute("UPDATE stocks SET available = ? WHERE symbol = ?", (available_supply, stock_name))
                await ctx.send(f"The available supply for all stocks within ETF {etf_id} has been set to {available_supply}.")

            self.conn.commit()

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while updating the ETF supply. Error: {str(e)}")


    @commands.command(name='simulate_buy')
    async def simulate_buy(self, ctx, symbol: str, amount: int):
        cursor = self.conn.cursor()

        # Fetch the stock price
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (symbol,))
        stock = cursor.fetchone()

        if not stock:
            await ctx.send(f"No stock found with symbol: {symbol}")
            return

        stock_price = float(stock[2])

        # Calculate Subtotal, Tax, and Total
        subtotal = stock_price * amount
        tax_percentage = get_tax_percentage(amount, subtotal)
        tax = subtotal * tax_percentage
        total = subtotal + tax

        # Calculate potential stock price after buy
        potential_price_increase = random.uniform(0.000001 * amount, min(0.000035 * amount, 1000000 - stock_price))
        potential_stock_price = min(stock_price + potential_price_increase, 1000000)

        # Create the embed
        embed = discord.Embed(
            title=f"Simulated Stock Purchase for {symbol}",
            color=discord.Color.green()
        )

        embed.add_field(name="Stock Price", value=f"{stock_price:.2f} coins", inline=True)
        embed.add_field(name="Subtotal", value=f"{subtotal:.2f} coins", inline=True)
        embed.add_field(name=f"Tax (at {tax_percentage*100:.1f}%)", value=f"{tax:.2f} coins", inline=True)
        embed.add_field(name="Total", value=f"{total:.2f} coins", inline=True)
        embed.add_field(name="Potential Stock Price After Buy", value=f"{potential_stock_price:.2f} coins", inline=False)

        await ctx.send(embed=embed)

    @commands.command(name="simulate_sell")
    @is_allowed_server(P3, SludgeSliders, OM3, PBL)
    async def simulate_sell(self, ctx, stock_name: str, amount: float):
        cursor = self.conn.cursor()

        # Get stock details
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
        stock = cursor.fetchone()

        if stock is None:
            await ctx.send(f"This stock does not exist.")
            return

        # Assuming `price` is the third column
        stock_price = float(stock[2])
        subtotal = stock_price * amount

        tax_percentage = get_tax_percentage(amount, subtotal)
        tax = subtotal * tax_percentage

        total = subtotal - tax  # Subtracting the tax for a sell

        # Calculate potential price decrease
        price_decrease = random.uniform(0.000011 * amount, min(0.00005 * amount, stock_price))
        potential_stock_price = max(stock_price - price_decrease, 0)  # Ensure the price doesn't go below 0

        # Embed creation
        embed = discord.Embed(title=f"Simulated Sell of {stock_name}", color=0x00FF00)
        embed.add_field(name="Stock Price", value=f"{stock_price:.2f} coins", inline=True)
        embed.add_field(name="Subtotal", value=f"{subtotal:.2f} coins", inline=True)
        embed.add_field(name=f"Tax (at {tax_percentage*100:.1f}%)", value=f"{tax:.2f} coins", inline=True)
        embed.add_field(name="Total After Tax", value=f"{total:.2f} coins", inline=True)
        embed.add_field(name="Potential Stock Price After Sale", value=f"{potential_stock_price:.2f} coins", inline=True)

        await ctx.send(embed=embed)

    @commands.command(name='roulette', help='Play roulette. Choose a color (red/black/green) or a number (0-36) or "even"/"odd" and your bet amount.')
    @is_allowed_server(P3, SludgeSliders, OM3, PBL)
    async def roulette(self, ctx, choice: str, bet: int):
        user_id = ctx.author.id
        current_balance = get_user_balance(self.conn, user_id)


        # Check if bet amount is positive
        if bet <= 0:
            await ctx.send(f"{ctx.author.mention}, bet amount must be a positive number.")
            return

        if bet > 500000:
            await ctx.send(f"{ctx.author.mention}, the maximum bet amount is 500,000 coins.")
            return

        if bet > current_balance:
            # Calculate the missing amount needed to complete the transaction including tax.
            missing_amount = bet - current_balance
            await ctx.send(f"{ctx.author.mention}, you do not have enough coins to place the bet. You need {missing_amount:.2f} more coins, including tax, to place this bet.")
            return

        # Initialize roulette wheel as numbers 0-36 with their associated colors
        red_numbers = list(range(1, 11, 2)) + list(range(12, 19, 2)) + list(range(19, 29, 2)) + list(range(30, 37, 2))
        wheel = [(str(i), 'red' if i in red_numbers else 'black' if i != 0 else 'green') for i in range(37)]

        # Spin the wheel
        spin_result, spin_color = random.choice(wheel)

        tax_percentage = get_tax_percentage(bet, current_balance)
        tax = (bet * Decimal(tax_percentage)) * Decimal(0.25)
        total_cost = bet + tax

        # Check for negative balance after tax
        if current_balance - total_cost < 0:
            await ctx.send(f"{ctx.author.mention}, you don't have enough coins to cover the bet and tax.")
            return

        # Deduct bet and tax from user's current balance
        new_balance = current_balance - total_cost
        update_user_balance(self.conn, user_id, new_balance)

        # Check if the user wins
        win_amount = 0
        if choice.lower() == spin_color:
            win_amount = bet * 2  # Payout for color choice is 2x
        elif choice.isdigit() and int(choice) == int(spin_result):
            win_amount = bet * 35  # Payout for number choice is 35x
        elif choice.lower() == "even" and int(spin_result) % 2 == 0 and spin_result != '0':
            win_amount = bet * 2  # Payout for 'even' is 2x
        elif choice.lower() == "odd" and int(spin_result) % 2 != 0:
            win_amount = bet * 2  # Payout for 'odd' is 2x

        if win_amount > 0:
            new_balance += win_amount
            update_user_balance(self.conn, user_id, new_balance)
            await ctx.send(f"{ctx.author.mention}, congratulations! The ball landed on {spin_result} ({spin_color}). You won {win_amount} coins. Your new balance is {new_balance}.")
            await log_gambling_transaction(ledger_conn, ctx, "Roulette", bet, f"You won {win_amount} coins", new_balance)
        else:
            await ctx.send(f"{ctx.author.mention}, the ball landed on {spin_result} ({spin_color}). You lost {total_cost} coins including tax. Your new balance is {new_balance}.")
            await log_gambling_transaction(ledger_conn, ctx, "Roulette", bet, f"You lost {total_cost} coins", new_balance)

    @commands.command(name='coinflip', help='Flip a coin and bet on heads or tails.')
    @is_allowed_server(P3, SludgeSliders, OM3, PBL)
    async def coinflip(self, ctx, choice: str, bet: int):
        user_id = ctx.author.id
        current_balance = get_user_balance(self.conn, user_id)

        # Check if bet amount is positive
        if bet <= 0:
            await ctx.send(f"{ctx.author.mention}, bet amount must be a positive number.")
            return

        if bet > 500000:
            await ctx.send(f"{ctx.author.mention}, the maximum bet amount is 500,000 coins.")
            return

        if bet > current_balance:
            # Calculate the missing amount needed to complete the transaction including tax.
            missing_amount = bet - current_balance
            await ctx.send(f"{ctx.author.mention}, you do not have enough coins to place the bet. You need {missing_amount:.2f} more coins, including tax, to place this bet.")
            return

        # Flip the coin
        coin_result = random.choice(['heads', 'tails'])

        tax_percentage = get_tax_percentage(bet, current_balance)
        tax = (bet * Decimal(tax_percentage)) * Decimal(0.25)
        total_cost = bet + tax

        # Check for negative balance after tax
        if current_balance - total_cost < 0:
            await ctx.send(f"{ctx.author.mention}, you don't have enough coins to cover the bet and tax.")
            return

        # Deduct bet and tax from user's current balance
        new_balance = current_balance - total_cost
        update_user_balance(self.conn, user_id, new_balance)

        # Check if the user wins
        if choice.lower() == coin_result:
            win_amount = bet * 2  # Payout for correct choice is 2x
            new_balance += win_amount
            update_user_balance(self.conn, user_id, new_balance)
            await ctx.send(f"{ctx.author.mention}, congratulations! The coin landed on {coin_result}. You won {win_amount} coins. Your new balance is {new_balance}.")
            await log_gambling_transaction(ledger_conn, ctx, "Coinflip", bet, f"You won {win_amount} coins", new_balance)
        else:
            await ctx.send(f"{ctx.author.mention}, the coin landed on {coin_result}. You lost {total_cost} coins including tax. Your new balance is {new_balance}.")
            await log_gambling_transaction(ledger_conn, ctx, "Coinflip", bet, f"You lost {total_cost} coins", new_balance)


    @commands.command(name='slotmachine', aliases=['slots'], help='Play the slot machine. Bet an amount up to 500,000 coins.')
    @is_allowed_server(P3, SludgeSliders, OM3, PBL)
    async def slotmachine(self, ctx, bet: int):
        user_id = ctx.author.id
        current_balance = get_user_balance(self.conn, user_id)

        # Check if bet amount is positive and within the limit
        if bet <= 0:
            await ctx.send(f"{ctx.author.mention}, bet amount must be a positive number.")
            return

        if bet > 500000:
            await ctx.send(f"{ctx.author.mention}, the maximum bet amount is 500,000 coins.")
            return

        # Define slot machine symbols and their values
        symbols = ["🍒", "🍋", "🍊", "🍇", "🔔", "💎", "7️⃣"]
        payouts = {"🍒": 5, "🍋": 10, "🍊": 15, "🍇": 20, "🔔": 25, "💎": 50, "7️⃣": 100}

        # Spin the slot machine
        result = [random.choice(symbols) for _ in range(3)]

        # Calculate the payout
        if all(symbol == result[0] for symbol in result):
            payout_multiplier = payouts[result[0]]
        elif result[0] == result[1] or result[1] == result[2]:
            payout_multiplier = 2.25  # 2 in a row with 2.25% payout
        else:
            payout_multiplier = 0

        win_amount = bet * payout_multiplier

        # Calculate tax based on the bet amount
        tax_percentage = get_tax_percentage(bet, current_balance)
        tax = (bet * Decimal(tax_percentage)) * Decimal(0.25)

        # Calculate total cost including tax
        total_cost = bet + tax

        # Check for negative balance after tax
        if current_balance - total_cost < 0:
            await ctx.send(f"{ctx.author.mention}, you don't have enough coins to cover the bet and tax.")
            return

        # Deduct the bet and tax from the user's current balance
        new_balance = current_balance - total_cost
        update_user_balance(self.conn, user_id, new_balance)

        # Create and send the embed with the slot machine result
        embed = discord.Embed(title="Slot Machine", color=discord.Color.gold())
        embed.add_field(name="Result", value=" ".join(result), inline=False)

        if win_amount > 0:
            new_balance += Decimal(win_amount)
            update_user_balance(self.conn, user_id, new_balance)
            embed.add_field(name="Congratulations!", value=f"You won {win_amount} coins!", inline=False)
            await log_gambling_transaction(ledger_conn, ctx, "Slots", bet, f"You won {win_amount} coins", new_balance)
        else:
            embed.add_field(name="Better luck next time!", value=f"You lost {total_cost} coins including tax. Your new balance is {new_balance} coins.", inline=False)
            await log_gambling_transaction(ledger_conn, ctx, "Slots", bet, f"You lost {total_cost} coins", new_balance)

        embed.set_footer(text=f"Your new balance: {new_balance} coins")
        await ctx.send(embed=embed)





    @commands.command(name='simulate_roulette', help='Simulate a roulette bet to see the amount in taxes it would cost, and how much you would win/lose.')
    @is_allowed_server(P3, SludgeSliders, OM3, PBL)
    async def simulate_roulette(self, ctx, bet: int):
        user_id = ctx.author.id
        current_balance = get_user_balance(self.conn, user_id)  # Placeholder function for fetching user's balance

        # Calculate tax and total cost
        tax_percentage = get_tax_percentage(bet, current_balance)  # Placeholder function for tax percentage
        tax = (bet * Decimal(tax_percentage)) * Decimal(0.25)
        total_cost = bet + tax

        # Calculate winnings for both color and number
        color_win = bet * 2
        number_win = bet * 35

        await ctx.send(f"{ctx.author.mention}, if you bet {bet} coins, here are the hypothetical outcomes:\n"
                       f"- Tax cost: {tax} coins\n"
                       f"- Total cost including tax: {total_cost} coins\n"
                       f"- Winnings if you chose the correct color: {color_win} coins\n"
                       f"- Winnings if you chose the correct number: {number_win} coins.")


    @commands.command(name='stats', help='Displays the user\'s financial stats.')
    async def stats(self, ctx):
        user_id = ctx.author.id
        conn = sqlite3.connect('currency_system.db')
        cursor = conn.cursor()
        try:
            # Get user balance
            cursor.execute("SELECT balance FROM users WHERE user_id=?", (user_id,))
            current_balance_row = cursor.fetchone()
            current_balance = current_balance_row[0] if current_balance_row else 0

            # Calculate total stock value
            cursor.execute("SELECT symbol, amount FROM user_stocks WHERE user_id=?", (user_id,))
            user_stocks = cursor.fetchall()
            total_stock_value = 0
            for symbol, amount in user_stocks:
                cursor.execute("SELECT price FROM stocks WHERE symbol=?", (symbol,))
                stock_price_row = cursor.fetchone()
                stock_price = stock_price_row[0] if stock_price_row else 0
                total_stock_value += stock_price * amount

            # Calculate total ETF value
            cursor.execute("SELECT etf_id, quantity FROM user_etfs WHERE user_id=?", (user_id,))
            user_etfs = cursor.fetchall()
            total_etf_value = 0
            for etf in user_etfs:
                etf_id = etf[0]
                quantity = etf[1]

                cursor.execute("SELECT SUM(stocks.price * etf_stocks.quantity) FROM etf_stocks JOIN stocks ON etf_stocks.symbol = stocks.symbol WHERE etf_stocks.etf_id=? GROUP BY etf_stocks.etf_id", (etf_id,))
                etf_value_row = cursor.fetchone()
                etf_value = etf_value_row[0] if etf_value_row else 0
                total_etf_value += (etf_value or 0) * quantity

            # Calculate total value of all funds
            total_funds_value = current_balance + total_stock_value + total_etf_value

            # Create the embed
            embed = Embed(title=f"{ctx.author.name}'s Financial Stats", color=Colour.green())
            embed.add_field(name="Balance", value=f"{current_balance:,.0f} coins", inline=False)
            embed.add_field(name="Total Stock Value", value=f"{total_stock_value:,.0f} coins", inline=False)
            embed.add_field(name="Total ETF Value", value=f"{total_etf_value:,.0f} coins", inline=False)
            embed.add_field(name="Total Funds Value", value=f"{total_funds_value:,.0f} coins", inline=False)

            await ctx.send(embed=embed)
            # Send the same embed to the ledger channel
            channel = ctx.guild.get_channel(ledger_channel)
            if channel:
                await channel.send(embed=embed)


        except sqlite3.Error as e:
            # Log error message for debugging
            print(f"Database error: {e}")

            # Inform the user that an error occurred
            await ctx.send(f"An error occurred while retrieving your stats. Please try again later.")

        except Exception as e:
            # Log error message for debugging
            print(f"An unexpected error occurred: {e}")

            # Inform the user that an error occurred
            await ctx.send(f"An unexpected error occurred. Please try again later.")



    @commands.command(name='check_stocks', help='Check which stocks you can still buy.')
    async def check_stocks(self, ctx):
        user_id = ctx.author.id

        # Connect to the database
        conn = sqlite3.connect("currency_system.db")
        cursor = conn.cursor()

        # Fetch all stock symbols and their available amounts
        cursor.execute("SELECT symbol, available FROM stocks")
        all_stocks = cursor.fetchall()

        stocks_can_buy = {}
        dStockLimit = 10000000
        for stock in all_stocks:
            stock_name = stock[0]
            stock_limit = dStockLimit

            # Get the total amount bought today by the user for this stock
            cursor.execute("""
                SELECT SUM(amount)
                FROM user_daily_buys
                WHERE user_id=? AND symbol=? AND DATE(timestamp)=DATE('now')
            """, (user_id, stock_name))

            daily_bought_record = cursor.fetchone()
            daily_bought = daily_bought_record[0] if daily_bought_record and daily_bought_record[0] is not None else 0

            # Calculate how many more the user can buy today
            remaining_amount = stock_limit - daily_bought

            if remaining_amount > 0:
                stocks_can_buy[stock_name] = remaining_amount

        # Close the connection automatically when exiting the `with` block
        conn.close()

        if stocks_can_buy:
            # Split the stocks into pages with a maximum of 5 stocks per page
            stock_pages = list(chunks(stocks_can_buy, 5))

            # Send the first page
            page_num = 0
            message = await ctx.send(embed=create_stock_page(stock_pages[page_num]), delete_after=60.0)

            # Add reactions for pagination
            if len(stock_pages) > 1:
                await message.add_reaction("◀️")
                await message.add_reaction("▶️")

            def check(reaction, user):
                return user == ctx.author and reaction.message.id == message.id

            while True:
                try:
                    reaction, _ = await self.bot.wait_for('reaction_add', timeout=60.0, check=check)

                    if str(reaction.emoji) == "◀️" and page_num > 0:
                        page_num -= 1
                        await message.edit(embed=create_stock_page(stock_pages[page_num]))
                    elif str(reaction.emoji) == "▶️" and page_num < len(stock_pages) - 1:
                        page_num += 1
                        await message.edit(embed=create_stock_page(stock_pages[page_num]))

                    await message.remove_reaction(reaction, ctx.author)

                except TimeoutError:
                    break
        else:
            message = f"{ctx.author.mention}, you have reached your daily limit for all available stocks."




    @commands.command(name="burn_stocks", help="Burn a certain amount of stocks to reduce total supply.")
    async def burn_stocks(self, ctx, stock_name: str, amount: int):
        user_id = ctx.author.id

        if amount <= 0:
            await ctx.send("Invalid amount. Please provide a positive number of stocks to burn.")
            return

        cursor = self.conn.cursor()

        # Check if the stock exists
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
        stock = cursor.fetchone()
        if stock is None:
            await ctx.send(f"This stock '{stock_name}' does not exist.")
            return

        # Check if the user has enough stocks to burn
        cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (user_id, stock_name))
        user_stock = cursor.fetchone()
        user_owned = int(user_stock[0]) if user_stock else 0

        if user_owned < amount:
            await ctx.send(f"You do not have enough {stock_name} stocks to burn.")
            return

        # Get the price before burning
        price_before_burn = Decimal(stock[2])

        # Update user's stocks by burning
        new_user_owned = user_owned - amount
        cursor.execute("UPDATE user_stocks SET amount=? WHERE user_id=? AND symbol=?", (new_user_owned, user_id, stock_name))

        # Update total supply by reducing the burned amount
        new_total_supply = stock["total_supply"] - amount
        cursor.execute("UPDATE stocks SET total_supply=? WHERE symbol=?", (new_total_supply, stock_name))

        # Increase Price by amount burned
        await self.increase_price(ctx, stock_name, amount)

        # Get the updated stock data after the price increase
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
        updated_stock = cursor.fetchone()
        price_after_burn = Decimal(updated_stock[2])  # Get the updated price

        # Log the stock burn transaction
        await log_burn_stocks(ledger_conn, ctx, stock_name, amount, price_before_burn, price_after_burn)
        self.conn.commit()

        await ctx.send(f"Burned {amount} {stock_name} stocks, reducing the total supply.")

    @commands.command(name="simulate_burn", help="Simulate burning a certain amount of stocks to reduce total supply.")
    async def simulate_burn(self, ctx, stock_name: str, amount: int):
        user_id = ctx.author.id

        if amount <= 0:
            await ctx.send("Invalid amount. Please provide a positive number of stocks to simulate burning.")
            return

        cursor = self.conn.cursor()

        # Check if the stock exists
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
        stock = cursor.fetchone()
        if stock is None:
            await ctx.send(f"This stock '{stock_name}' does not exist.")
            return

        # Calculate the potential price after simulating the burn
        current_total_supply = int(stock[3])
        price_before_simulated_burn = Decimal(stock[2])

        # Use the same logic as in the `increase_price` command
        price_increase = random.uniform(buyPressureMin * amount, min(buyPressureMax * amount, 1000000 - float(stock[2])))
        new_price = min(float(stock[2]) + price_increase, stockMax)

        # Calculate the potential total supply after simulating the burn
        potential_total_supply = current_total_supply - amount

        # Calculate the potential price after the burn
        potential_price_after_burn = Decimal(new_price)

        # Create an embed for the simulation results
        embed = discord.Embed(
            title=f"Simulation: Burn {amount} {stock_name} Stocks",
            color=discord.Color.blue()
        )
        embed.add_field(name="Price Before Simulation", value=f"{price_before_simulated_burn}", inline=False)
        embed.add_field(name="Potential Price After Simulation", value=f"{potential_price_after_burn}", inline=False)
        embed.set_footer(text=f"Total Supply After Simulation: {potential_total_supply}")

        await ctx.send(embed=embed)


    @commands.command(name="swap_stocks", help="Swap stocks dynamically, adjusting value through balance.")
    @is_allowed_user(930513222820331590, PBot)
    async def swap_stocks(self, ctx, stock1_name: str, amount1: int, stock2_name: str, amount2: int):
        user_id = ctx.author.id
        cursor = self.conn.cursor()

        # Validate input and fetch stock prices and user's stock holdings
        cursor.execute("SELECT amount, price FROM user_stocks JOIN stocks ON user_stocks.symbol=stocks.symbol WHERE user_id=? AND user_stocks.symbol=?", (user_id, stock1_name))
        user_stock1 = cursor.fetchone()
        cursor.execute("SELECT amount, price FROM user_stocks JOIN stocks ON user_stocks.symbol=stocks.symbol WHERE user_id=? AND user_stocks.symbol=?", (user_id, stock2_name))
        user_stock2 = cursor.fetchone()

        if not user_stock1 or not user_stock2:
            await ctx.send("One or both of the specified stocks do not exist in your holdings.")
            return

        stock1_amount, stock1_price = user_stock1[0], user_stock1[1]
        stock2_amount, stock2_price = user_stock2[0], user_stock2[1]

        value1 = stock1_price * amount1
        value2 = stock2_price * amount2

        difference = value1 - value2

        if abs(difference) <= swapThreshold:  # Define a small threshold for value comparison
            # Perform the stock swap
            new_stock1_amount = stock1_amount - amount1 + amount2
            new_stock2_amount = stock2_amount - amount2 + amount1

            cursor.execute("UPDATE user_stocks SET amount=? WHERE user_id=? AND symbol=?", (new_stock1_amount, user_id, stock1_name))
            cursor.execute("UPDATE user_stocks SET amount=? WHERE user_id=? AND symbol=?", (new_stock2_amount, user_id, stock2_name))

            # Deduct the remaining difference from the user's balance
            current_balance = get_user_balance(self.conn, user_id)
            remaining_difference_decimal = Decimal(remaining_difference)  # Convert to Decimal
            new_balance = current_balance - remaining_difference_decimal
            update_user_balance(self.conn, user_id, new_balance)

            self.conn.commit()

            await ctx.send(f"Successfully swapped {amount1} {stock1_name} stocks for {amount2} {stock2_name} stocks with value adjustment. Your balance has been adjusted by {remaining_difference:.2f} coins.")
        else:
            await ctx.send(f"The difference in values is too significant for a direct swap. You need to adjust the value through your balance.")


    @commands.command(name="check_swap_amounts", help="Check how much of a stock can be swapped for another.")
    async def check_swap_amounts(self, ctx, target_stock_name: str):
        user_id = ctx.author.id
        cursor = self.conn.cursor()

        # Get the target stock's price
        cursor.execute("SELECT price FROM stocks WHERE symbol=?", (target_stock_name,))
        target_stock = cursor.fetchone()

        if not target_stock:
            await ctx.send(f"The specified target stock '{target_stock_name}' does not exist.")
            return

        target_price = target_stock[0]

        # Get user's stock holdings
        cursor.execute("SELECT amount, price, user_stocks.symbol FROM user_stocks JOIN stocks ON user_stocks.symbol=stocks.symbol WHERE user_id=?", (user_id,))
        user_stocks = cursor.fetchall()

        valid_swaps = []

        for user_stock in user_stocks:
            source_amount, source_price, source_stock_name = user_stock
            gained_target_amount = round(source_amount * target_price / source_price)
            remaining_source_amount = source_amount - gained_target_amount

            value_difference = abs((source_amount * source_price) - (gained_target_amount * target_price))

            if remaining_source_amount >= 0 and value_difference <= swapThreshold:
                valid_swaps.append((source_amount, source_stock_name, gained_target_amount, remaining_source_amount))

        if valid_swaps:
            swap_info = f"Swap options for your stocks to {target_stock_name}:\n"
            for swap in valid_swaps:
                source_amount, source_stock_name, gained_target_amount, remaining_source_amount = swap
                swap_info += f"Swap {source_amount} {source_stock_name} for {gained_target_amount} {target_stock_name}. Remaining: {remaining_source_amount} {source_stock_name}\n"
            await ctx.send(swap_info)
        else:
            await ctx.send(f"No valid swaps found for your stocks to {target_stock_name}.")



async def setup(bot):
    await bot.add_cog(CurrencySystem(bot))
