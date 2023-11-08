from discord.ext import commands, tasks
from discord import Embed, Colour
from tabulate import tabulate
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from matplotlib.ticker import FuncFormatter
from discord.utils import get
from math import ceil, floor
from sqlite3.dbapi2 import Connection, Cursor
from contextlib import contextmanager
from functools import partial
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import mplfinance as mpf
import numpy as np
import decimal
import sqlite3
import random
import discord
import time
import asyncio
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
import talib
import hashlib
import shlex





# Hardcoded Variables

announcement_channel_ids = [1093540470593962014, 1124784361766650026, 1124414952812326962]
stockMin = 1000
stockMax = 5000000
dStockLimit = 150000000 #2000000 standard
dETFLimit = 500000000000
treasureMin = 10000000
treasureMax = 50000000
MAX_BALANCE = Decimal('750000000000000000')
sellPressureMin = 0.000025
sellPressureMax = 0.000055
buyPressureMin = 0.0000055
buyPressureMax = 0.0000850
stockDecayValue = 0.00025
decayMin = 0.01
resetµPPN = 100
dailyMin = 100000
dailyMax = 500000
ticketPrice = 100
maxTax = 0.50
minBet = 10000
maxBet = 500000000
last_buy_time = {}
last_sell_time = {}

## One-time pass roles

bronze_pass = 1162473766656413987
role_discount = 0.05

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
ledger_channel = 1161680453841993839
#servers

P3 = 1087147399371292732
SludgeSliders = 1121529633448394973
OM3 = 1070860008868302858
PBL = 1132202921589739540
server1 = 1078544683971661944


GUILD_ID = 1161678765894664323
allowedServers = P3, SludgeSliders, OM3, PBL
##

## WORK
# Define your token earning range
MIN_EARNINGS = 25000
MAX_EARNINGS = 500000



## End Work


## BalckJack Helpers and Functions

def calculate_hand_value(hand):
    value = 0
    num_aces = 0

    for card in hand:
        card_value = card[:-1]

        if card_value in ['J', 'Q', 'K']:
            value += 10
        elif card_value == 'A':
            value += 11
            num_aces += 1
        else:
            try:
                value += int(card_value)
            except ValueError:
                # Handle non-numeric card values (e.g., '4♣')
                value += 10

    # Adjust for aces
    while value > 21 and num_aces:
        value -= 10
        num_aces -= 1

    return value

# Helper functions for handling different outcomes in Blackjack
async def handle_blackjack_win(ctx, user_id, bet, current_balance):
    currency_conn = sqlite3.connect("currency_system.db")
    win_amount = bet * 2
    new_balance = current_balance + Decimal(win_amount)
    update_user_balance(currency_conn, user_id, new_balance)

    embed = discord.Embed(title="Blackjack - You Win!", color=discord.Color.green())
    embed.add_field(name="Your Hand", value=", ".join(player_hand) + f" ({player_value})", inline=False)
    embed.add_field(name="Dealer's Hand", value=", ".join(dealer_hand) + f" ({dealer_value})", inline=False)
    embed.add_field(name="Congratulations!", value=f"You won {win_amount} µPPN!", inline=False)
    embed.set_footer(text=f"Your new balance: {new_balance:,.2f} µPPN")
    await ctx.send(embed=embed)

async def handle_blackjack_loss(ctx, user_id, bet, current_balance):
    currency_conn = sqlite3.connect("currency_system.db")
    new_balance = current_balance - Decimal(bet)
    update_user_balance(currency_conn, user_id, new_balance)

    embed = discord.Embed(title="Blackjack - You Lose!", color=discord.Color.red())
    embed.add_field(name="Your Hand", value=", ".join(player_hand) + f" ({player_value})", inline=False)
    embed.add_field(name="Dealer's Hand", value=", ".join(dealer_hand) + f" ({dealer_value})", inline=False)
    embed.add_field(name="Better luck next time!", value=f"You lost {bet} µPPN.", inline=False)
    embed.set_footer(text=f"Your new balance: {new_balance:,.2f} µPPN")
    await ctx.send(embed=embed)

async def handle_blackjack_push(ctx, user_id, bet, current_balance):
    currency_conn = sqlite3.connect("currency_system.db")
    new_balance = current_balance
    update_user_balance(currency_conn, user_id, new_balance)

    embed = discord.Embed(title="Blackjack - Push!", color=discord.Color.blue())
    embed.add_field(name="Your Hand", value=", ".join(player_hand) + f" ({player_value})", inline=False)
    embed.add_field(name="Dealer's Hand", value=", ".join(dealer_hand) + f" ({dealer_value})", inline=False)
    embed.add_field(name="It's a tie!", value=f"No µPPN gained or lost.", inline=False)
    embed.set_footer(text=f"Your balance remains: {new_balance:,.2f} µPPN")
    await ctx.send(embed=embed)


def parse_time_shorthand(shorthand):
    if shorthand.endswith("m"):
        return timedelta(minutes=int(shorthand[:-1]))
    elif shorthand.endswith("h"):
        return timedelta(hours=int(shorthand[:-1]))
    elif shorthand.endswith("d"):
        return timedelta(days=int(shorthand[:-1]))
    else:
        raise ValueError("Invalid time shorthand. Use 'm' for minutes, 'h' for hours, or 'd' for days.")

def calculate_remaining_time(last_action_time, time_threshold):
    if last_action_time is None:
        return timedelta()

    time_elapsed = datetime.now() - last_action_time
    remaining_time = max(time_threshold - time_elapsed, timedelta())
    remaining_time -= timedelta(hours=4)
    return remaining_time

def buy_check(cursor, user_id, stock_name, time_threshold):
    # Convert shorthand time to timedelta
    time_threshold = parse_time_shorthand(time_threshold)

    # Get the total amount bought within the specified time threshold by the user for this stock
    cursor.execute("""
        SELECT SUM(amount), MAX(timestamp)
        FROM user_daily_buys
        WHERE user_id=? AND symbol=? AND timestamp >= ?
    """, (user_id, stock_name, datetime.now() - time_threshold))

    daily_bought_record = cursor.fetchone()
    daily_bought = daily_bought_record[0] if daily_bought_record and daily_bought_record[0] is not None else 0
    last_purchase_time_str = daily_bought_record[1] if daily_bought_record and daily_bought_record[1] is not None else None

    # Convert last_purchase_time to a datetime object
    last_purchase_time = datetime.strptime(last_purchase_time_str, "%Y-%m-%d %H:%M:%S") if last_purchase_time_str else None

    remaining_time = calculate_remaining_time(last_purchase_time, time_threshold)

    return daily_bought > 0, last_purchase_time, remaining_time

def sell_check(cursor, user_id, stock_name, time_threshold):
    # Convert shorthand time to timedelta
    time_threshold = parse_time_shorthand(time_threshold)

    # Get the total amount sold within the specified time threshold by the user for this stock
    cursor.execute("""
        SELECT SUM(amount), MAX(timestamp)
        FROM user_daily_sells
        WHERE user_id=? AND symbol=? AND timestamp >= ?
    """, (user_id, stock_name, datetime.now() - time_threshold))

    daily_sold_record = cursor.fetchone()
    daily_sold = daily_sold_record[0] if daily_sold_record and daily_sold_record[0] is not None else 0
    last_sold_time_str = daily_sold_record[1] if daily_sold_record and daily_sold_record[1] is not None else None

    # Convert last_sold_time to a datetime object
    last_sold_time = datetime.strptime(last_sold_time_str, "%Y-%m-%d %H:%M:%S") if last_sold_time_str else None

    remaining_time = calculate_remaining_time(last_sold_time, time_threshold)

    return daily_sold > 0, last_sold_time, remaining_time



def is_valid_user_id(client, user_id):
    user = client.get_user(user_id)
    return user is not None

#client = discord.Client()

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

def create_multipage_embeds(data, title):
    # Split data into chunks to create multipage embeds
    chunk_size = 10  # Adjust the chunk size as needed
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

    # Create multipage embeds
    pages = []
    for chunk in chunks:
        embed = discord.Embed(title=title, color=discord.Color.blue())
        for action, symbol, total_quantity, total_pre_tax_amount in chunk:
            embed.add_field(
                name=f"{action.capitalize()} {symbol}",
                value=f"Total Quantity: {total_quantity}\nTotal Pre-tax Amount: {total_pre_tax_amount:,.0f} µPPN",
                inline=True
            )
        pages.append(embed)

    return pages




async def check_impact(cursor, ctx, stock_name: str, amount: float):

    # Get stock details
    cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
    stock = cursor.fetchone()

    if stock is None:
        await ctx.send(f"This stock does not exist.")
        return

    # Assuming `price` is the third column
    stock_price = float(stock[2])
    subtotal = stock_price * amount


    # Calculate potential price decrease
    price_decrease = random.uniform(sellPressureMin * amount, min(sellPressureMax * amount, stock_price))
    potential_stock_price = max(stock_price - price_decrease, 0)  # Ensure the price doesn't go below 0

    # Calculate impact percentage
    impact_percentage = ((stock_price - potential_stock_price) / stock_price) * 100
    print(f"{generate_crypto_address(ctx.author.id)}: Selling {amount:,} shares of {stock_name} with an impact of {impact_percentage:2f}%")

    # Check if impact percentage is over 10%
    if impact_percentage > 10:
        confirmation_message = await ctx.send(
            f"The impact of this sell is estimated to be {impact_percentage}%. "
            f"Are you sure you want to continue? Type `confirm` and press enter to proceed."
        )

        try:
            # Wait for user confirmation
            confirm_response = await ctx.bot.wait_for(
                "message",
                check=lambda msg: msg.author == ctx.author and msg.channel == ctx.channel and msg.content.lower() == "confirm",
                timeout=30,
            )

            # Check if the user confirmed
            if confirm_response:
                await ctx.send("Sell confirmed. Proceeding with the sell.")
            else:
                await ctx.send("Sell canceled.")
                return
        except asyncio.TimeoutError:
            await ctx.send("Sell confirmation timed out. Sell canceled.")
            return

async def check_and_notify_address(ctx):
    # Connect to the P3addr.db database
    conn = sqlite3.connect("P3addr.db")

    # Get the user's ID
    user_id = str(ctx.author.id)

    # Check if the user has already stored an address
    if has_stored_address(conn, user_id):
        await ctx.send("You have already stored a P3 address.")
        conn.close()
        return

    # Notify the user to store an address
    await ctx.send("You have not stored a P3 address. Please use `!store_addr` to store your P3 address.")

    # Close the database connection
    conn.close()

async def get_stock_price(conn, stock_name):
    cursor = conn.cursor()
    cursor.execute("SELECT price FROM stocks WHERE symbol=?", (stock_name,))
    result = cursor.fetchone()
    return result[0] if result else None


async def reset_daily_stock_limits(ctx, user_id):
    # Connect to the currency_system database
    currency_conn = sqlite3.connect("currency_system.db")
    cursor = currency_conn.cursor()

    try:
        # Check if the user has a daily stock buy record for any stock
        cursor.execute("""
            SELECT DISTINCT symbol
            FROM user_daily_buys
            WHERE user_id=? AND timestamp >= date('now', '-1 day')
        """, (user_id,))
        stocks = cursor.fetchall()

        if stocks:
            # Reset the daily stock buy record for all stocks
            cursor.execute("""
                DELETE FROM user_daily_buys
                WHERE user_id=? AND timestamp >= date('now', '-1 day')
            """, (user_id,))
            currency_conn.commit()

            stock_symbols = ', '.join(stock[0] for stock in stocks)
            await ctx.send(f"Successfully reset daily stock buy limits for the user with ID {user_id} and stocks: {stock_symbols}.")
        else:
            await ctx.send(f"This user did not reach the daily stock buy limit for any stocks yet.")
    except sqlite3.Error as e:
        await ctx.send(f"An error occurred: {str(e)}")
    finally:
        # Close the database connection
        currency_conn.close()



async def reset_daily_burn_limits(ctx, user_id):
    # Connect to the currency_system database
    currency_conn = sqlite3.connect("currency_system.db")
    cursor = currency_conn.cursor()

    try:
        # Check if the user has a burn history record
        cursor.execute("SELECT * FROM burn_history WHERE user_id=? AND timestamp >= date('now', '-1 day')", (user_id,))
        burn_history_record = cursor.fetchone()

        if burn_history_record:
            # Reset the burn history
            cursor.execute("DELETE FROM burn_history WHERE user_id=?", (user_id,))
            currency_conn.commit()

            await ctx.send(f"Successfully reset burn limit for the user with ID {user_id}.")
        else:
            await ctx.send("This user did not reach the daily burn limit yet.")
    except sqlite3.Error as e:
        await ctx.send(f"An error occurred: {str(e)}")
    finally:
        # Close the database connection
        currency_conn.close()


def can_burn_stocks(ctx, cursor, user_id):
    # Check if the user can burn stocks based on the daily limit
    member = ctx.guild.get_member(user_id)
    today = datetime.today().date()
    cursor.execute("SELECT COUNT(*) FROM burn_history WHERE user_id=? AND timestamp >= ?", (user_id, today))
    daily_burn_count = cursor.fetchone()[0]
    if has_role(member, bronze_pass):
        return daily_burn_count < 10  # Adjust the daily limit as needed
    else:
        return daily_burn_count < 5

def update_burn_history(cursor, user_id):
    # Update the user's burn history
    cursor.execute("INSERT INTO burn_history (user_id, timestamp) VALUES (?, CURRENT_TIMESTAMP)", (user_id,))

##


# Function to format y-axis as currency
def currency_formatter(x, pos):
    return f"${x:,.2f}"

## Begin Ledger

# Register a custom adapter for decimal.Decimal
sqlite3.register_adapter(Decimal, lambda d: str(d))


# Function to create a connection to the SQLite database
def create_connection():
    return sqlite3.connect("p3ledger.db")

ledger_conn = create_connection()

async def log_transaction(ledger_conn, ctx, action, symbol, quantity, pre_tax_amount, post_tax_amount, balance_before, balance_after, price):
    # Get the user's username from the context
    if balance_before == 0 and balance_after == 0:
        P3Addr = "P3:03da907038"
    else:
        username = ctx.author.name
        P3Addr = generate_crypto_address(ctx.author.id)

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

    # Replace GUILD_ID with the actual ID of your guild
    guild_id = 1161678765894664323
    guild = ctx.bot.get_guild(guild_id)

    if guild:
        # Replace CHANNEL_ID with the actual ID of your logging channels
        channel1_id = ledger_channel

        # Get the channels from the guild
        channel1 = guild.get_channel(channel1_id)

        if channel1:
            # Create an embed for the log message
            embed = discord.Embed(
                title=f"{P3Addr} {action} {symbol} {'ETF' if is_etf else 'Stock'} Transaction",
                description=f"Quantity: {quantity}\n"
                            f"Price: {price} µPPN\n"
                            f"Pre-tax Amount: {pre_tax_amount:,.2f} µPPN\n"
                            f"Post-tax Amount: {post_tax_amount:,.2f} µPPN\n"
                            f"Balance Before: {balance_before:,.2f} µPPN\n"
                            f"Balance After: {balance_after:,.2f} µPPN\n"
                            f"Timestamp: {timestamp}",
                color=discord.Color.green() if action.startswith("Buy") else discord.Color.red()
            )

            # Send the log message as an embed to the specified channels
            await channel1.send(embed=embed)







async def log_transfer(ledger_conn, ctx, sender_name, receiver_name, receiver_id, amount):
    guild_id = 1161678765894664323
    guild = ctx.bot.get_guild(guild_id)

    if guild:
        P3Addr_sender = generate_crypto_address(ctx.author.id)
        P3Addr_receiver = generate_crypto_address(receiver_id)

        cursor = ledger_conn.cursor()
        cursor.execute("""
            INSERT INTO transfer_transactions (sender_id, receiver_id, amount)
            VALUES (?, ?, ?)
        """, (ctx.author.id, receiver_id, amount))
        ledger_conn.commit()

        channel1_id = ledger_channel

        channel1 = guild.get_channel(channel1_id)

        if channel1:
            embed = discord.Embed(
                title=f"Transfer from {P3Addr_sender} to {P3Addr_receiver}",
                description=f"Amount: {amount:,.2f} µPPN",
                color=discord.Color.purple()
            )

            await channel1.send(embed=embed)



async def log_stock_transfer(ledger_conn, ctx, sender, receiver, symbol, amount):
    try:
        guild_id = 1161678765894664323
        guild = ctx.bot.get_guild(guild_id)

        if amount == 0.0:
            return

        if guild:
            if sender == "stakingYield":
                P3Addr_sender = "P3:0x00000000"
            else:
                P3Addr_sender = generate_crypto_address(sender.id)
            if isinstance(receiver, int) == True:
                P3Addr_receiver = generate_crypto_address(receiver)
            else:
                P3Addr_receiver = generate_crypto_address(receiver.id)

            channel1_id = ledger_channel

            channel1 = guild.get_channel(channel1_id)

            if channel1:
                embed = discord.Embed(
                    title=f"Stock Transfer from {P3Addr_sender} to {P3Addr_receiver}",
                    description=f"Stock: {symbol}\nAmount: {amount:,.2f}",
                    color=discord.Color.purple()
                )

                await channel1.send(embed=embed)


    except Exception as e:
        print(f"Error while logging stock transfer: {e}")


async def log_item_transaction(conn, ctx, action, item_name, quantity, total_cost, tax_amount, new_balance):
    guild_id = 1161678765894664323
    guild = ctx.bot.get_guild(guild_id)

    if guild:
        P3Addr = generate_crypto_address(ctx.author.id)

        total_cost_str = str(total_cost)
        tax_amount_str = str(tax_amount)
        new_balance_str = str(new_balance)

        timestamp = ctx.message.created_at.strftime("%Y-%m-%d %H:%M:%S")

        is_purchase = True if action == "Buy" else False

        channel1_id = ledger_channel


        channel1 = guild.get_channel(channel1_id)


        if channel1:
            embed = discord.Embed(
                title=f"{P3Addr} {'Purchase' if is_purchase else 'Sale'} of {quantity:,.2f} {item_name}",
                description=f"Item: {item_name}\nQuantity: {quantity:,.2f}\nTotal Cost: {total_cost:,.2f} µPPN\n"
                            f"Tax Amount: {tax_amount:,.2f} µPPN\nNew Balance: {new_balance:,.2f} µPPN\nTimestamp: {timestamp}",
                color=discord.Color.green() if is_purchase else discord.Color.red()
            )

            await channel1.send(embed=embed)



async def log_burn_stocks(ledger_conn, ctx, stock_name, quantity, price_before, price_after):
    guild_id = 1161678765894664323
    guild = ctx.bot.get_guild(guild_id)

    if guild:
        P3Addr = generate_crypto_address(ctx.author.id)

        timestamp = ctx.message.created_at.strftime("%Y-%m-%d %H:%M:%S")

        conn = create_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO stock_burning_transactions (user_id, stock_name, quantity, price_before, price_after)
            VALUES (?, ?, ?, ?, ?)
        """, (ctx.author.id, stock_name, quantity, price_before, price_after))
        conn.commit()
        conn.close()

        channel1_id = ledger_channel


        channel1 = guild.get_channel(channel1_id)


        if channel1:
            embed = discord.Embed(
                title=f"{P3Addr} Stock Burn Transaction",
                description=f"Stock Name: {stock_name}\nQuantity Burned: {quantity:,.2f}\n"
                            f"Price Before Burn: {price_before:,.2f} µPPN\nPrice After Burn: {price_after:,.2f} µPPN\nTimestamp: {timestamp}",
                color=discord.Color.yellow()
            )

            await channel1.send(embed=embed)



async def log_gambling_transaction(ledger_conn, ctx, game, bet_amount, win_loss, amount_after_tax):
    guild_id = 1161678765894664323
    guild = ctx.bot.get_guild(guild_id)

    if guild:
        P3Addr = generate_crypto_address(ctx.author.id)

        timestamp = ctx.message.created_at.strftime("%Y-%m-%d %H:%M:%S")

        conn = create_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO gambling_transactions (user_id, game, bet_amount, win_loss, amount_after_tax)
            VALUES (?, ?, ?, ?, ?)
        """, (ctx.author.id, game, bet_amount, win_loss, amount_after_tax))
        conn.commit()
        conn.close()

        channel1_id = ledger_channel

        channel1 = guild.get_channel(channel1_id)

        if channel1:
            embed = discord.Embed(
                title=f"{P3Addr} {game} Gambling Transaction",
                description=f"Game: {game}\nBet Amount: {bet_amount} µPPN\nWin/Loss: {win_loss}\n"
                            f"Amount Paid/Received After Taxes: {amount_after_tax:,.2f} µPPN",
                color=discord.Color.orange() if win_loss.startswith("You won") else discord.Color.orange()
            )

            await channel1.send(embed=embed)

## End Ledger

def generate_crypto_address(user_id):
    # Concatenate a prefix (for simulation purposes) with the user ID
    data = f"P3:{user_id}"

    # Use a hash function (SHA-256) to generate a pseudo crypto address
    hashed_data = hashlib.sha256(data.encode()).hexdigest()[:10]

    return f"P3:{hashed_data}"

# Function to check if the user has already stored an address
def has_stored_address(conn, user_id):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM user_addresses WHERE user_id=?", (str(user_id),))
    return cursor.fetchone() is not None

def is_vanity_address_unique(conn, vanity_address):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM user_addresses WHERE vanity_address=?", (vanity_address,))
    count = cursor.fetchone()[0]
    return count == 0

def get_vanity_address(conn, user_id):
    cursor = conn.cursor()
    cursor.execute("SELECT vanity_address FROM user_addresses WHERE user_id=?", (user_id,))
    result = cursor.fetchone()
    return result[0] if result else None

# Function to get P3 address from user ID
def get_p3_address(conn, user_id):
    cursor = conn.cursor()
    cursor.execute("SELECT p3_address FROM user_addresses WHERE user_id=?", (str(user_id),))
    result = cursor.fetchone()
    return result[0] if result else None

# Function to get user ID from P3 address
def get_user_id(conn, p3_address):
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM user_addresses WHERE p3_address=?", (p3_address,))
    result = cursor.fetchone()
    return int(result[0]) if result else None

def get_user_id_from_discord_username(conn, discord_username):
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM user_addresses WHERE p3_address=?", (discord_username,))
    result = cursor.fetchone()
    return result[0] if result else None

def get_user_id_from_input(conn, target):
    cursor = conn.cursor()

    # Check if the target is a mention
    if target.startswith('<@') and target.endswith('>'):
        target_id = int(target[2:-1])
        return str(target_id)

    # Check if the target is a P3 address
    cursor.execute("SELECT user_id FROM user_addresses WHERE p3_address=?", (target,))
    result = cursor.fetchone()
    if result:
        return result[0]

    # Assume the target is a user ID
    return target


def get_user_id_by_vanity(conn, vanity_address):
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM user_addresses WHERE vanity_address=?", (vanity_address,))
    result = cursor.fetchone()
    return result[0] if result else None


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



def create_vip_table():
    conn = sqlite3.connect("VIP.db")
    cursor = conn.cursor()

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS etf_transactions (
                transaction_id INTEGER PRIMARY KEY,
                user_id TEXT,
                etf_id INTEGER,
                transaction_type TEXT,
                amount REAL,
                tax_amount REAL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
    except sqlite3.Error as e:
        # Handle any errors, if necessary
        print(f"Error creating etf_transactions table: {e}")
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS etf_percentages (
                role_id INTEGER PRIMARY KEY,
                role_name TEXT,
                percentage REAL
            )
        """)
    except sqlite3.Error as e:
        # Handle any errors, if necessary
        print(f"Error creating etf_percentages table: {e}")

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS treasure_percentages (
                role_id INTEGER PRIMARY KEY,
                role_name TEXT,
                percentage REAL
            )
        """)
    except sqlite3.Error as e:
        # Handle any errors, if necessary
        print(f"Error creating treasure_percentages table: {e}")


    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_stock_percentages (
                role_id INTEGER PRIMARY KEY,
                role_name TEXT,
                percentage REAL
            )
        """)
    except sqlite3.Error as e:
        # Handle any errors, if necessary
        print(f"Error creating daily_stock_percentages table: {e}")

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weekly_tax_distribution (
                distribution_id INTEGER PRIMARY KEY,
                etf_id INTEGER,
                role_id INTEGER,
                total_tax REAL,
                distributed_amount REAL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
    except sqlite3.Error as e:
        # Handle any errors, if necessary
        print(f"Error creating weekly_tax_distribution table: {e}")
    finally:
        conn.commit()
        conn.close()


def create_p3addr_table():
    # Connect to the database
    conn = sqlite3.connect("P3addr.db")

    # Create a cursor object to execute SQL commands
    cursor = conn.cursor()

    # Execute the SQL command to create the table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_addresses (
            user_id TEXT PRIMARY KEY,
            p3_address TEXT NOT NULL,
            vanity_address TEXT NOT NULL DEFAULT ''
        )
    ''')

    # Commit the changes and close the connection
    print("P3 Address Database Created")
    conn.commit()
    conn.close()

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
                µPPN_required INTEGER NOT NULL,
                µPPN_rewarded INTEGER NOT NULL
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
        print("Stock Limit created successfully")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the tables: {str(e)}")

    try:
        cursor.execute("""

            CREATE TABLE IF NOT EXISTS user_daily_sells (
                user_id INTEGER,
                symbol TEXT,
                amount INTEGER,
                timestamp DATETIME
            );
        """)
        print("Daily Sells Limit created successfully")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the tables: {str(e)}")


    try:
        cursor.execute("""

            CREATE TABLE IF NOT EXISTS burn_history (
                user_id TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        print("Burn Limit created successfully")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the tables: {str(e)}")

    try:
        cursor.execute("""

            CREATE TABLE IF NOT EXISTS item_usage (
                user_id TEXT NOT NULL,
                item_name TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, item_name)
            );
        """)
        print("Item limit created successfully")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the tables: {str(e)}")


    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS swap_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                stock1 TEXT,
                amount1 INTEGER,
                stock2 TEXT,
                amount2 INTEGER,
                status TEXT CHECK (status IN ('open', 'matched')),
                FOREIGN KEY (stock1) REFERENCES stocks(symbol),
                FOREIGN KEY (stock2) REFERENCES stocks(symbol)
            )
        """)
        print("Swap Orders created successfully")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the tables: {str(e)}")

    conn.commit()
    return conn


setup_database()
setup_ledger()
create_p3addr_table()
create_vip_table()

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


@staticmethod
def execute_stock_swap(user_order, matched_order):
    user_id = user_order[1]
    stock1, amount1, stock2, amount2 = user_order[2], user_order[3], user_order[4], user_order[5]
    matched_user_id = matched_order[1]
    matched_stock1, matched_amount1, matched_stock2, matched_amount2 = matched_order[2], matched_order[3], matched_order[4], matched_order[5]

    cursor = self.conn.cursor()

    try:
        # Fetch user's stock holdings
        cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (user_id, stock1))
        user_stock1_amount = cursor.fetchone()[0]
        cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (user_id, stock2))
        user_stock2_amount = cursor.fetchone()[0]

        # Fetch matched user's stock holdings
        cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (matched_user_id, matched_stock1))
        matched_user_stock1_amount = cursor.fetchone()[0]
        cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (matched_user_id, matched_stock2))
        matched_user_stock2_amount = cursor.fetchone()[0]

        # Update user's stock holdings
        cursor.execute("UPDATE user_stocks SET amount=? WHERE user_id=? AND symbol=?", (user_stock1_amount - amount1 + amount2, user_id, stock1))
        cursor.execute("UPDATE user_stocks SET amount=? WHERE user_id=? AND symbol=?", (user_stock2_amount - amount2 + amount1, user_id, stock2))

        # Update matched user's stock holdings
        cursor.execute("UPDATE user_stocks SET amount=? WHERE user_id=? AND symbol=?", (matched_user_stock1_amount - matched_amount1 + matched_amount2, matched_user_id, matched_stock1))
        cursor.execute("UPDATE user_stocks SET amount=? WHERE user_id=? AND symbol=?", (matched_user_stock2_amount - matched_amount2 + matched_amount1, matched_user_id, matched_stock2))

        # Commit the changes
        self.conn.commit()

        print("Stock swap executed successfully.")

    except sqlite3.Error as e:
        # Handle database errors
        print(f"Database error during stock swap: {e}")

    except Exception as e:
        # Handle unexpected errors
        print(f"Unexpected error during stock swap: {e}")


def is_allowed_user(*user_ids):
    async def predicate(ctx):
        return ctx.author.id in user_ids
    return commands.check(predicate)

def is_allowed_server(*server_ids):
    async def predicate(ctx):
        return ctx.guild.id in server_ids
    return commands.check(predicate)


def insert_raffle_tickets(conn, user_id, quantity, timestamp):
    cursor = conn.cursor()
    cursor.execute("INSERT INTO raffle_tickets (user_id, quantity, timestamp) VALUES (?, ?, ?, ?)",
                   (user_id, quantity, timestamp))
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
    try:
        if not isinstance(user_id, int) or new_balance is None:
            raise ValueError(f"Invalid user_id or new_balance value. user_id: {user_id}, new_balance: {new_balance:,.2f}")

        # Ensure that new_balance is a Decimal
        new_balance = Decimal(new_balance)

        # Check if new_balance exceeds the maximum allowed
        if user_id != PBot and user_id != jacob and new_balance > MAX_BALANCE:
            raise ValueError(f"User balance exceeds the maximum allowed balance of {MAX_BALANCE:,.2f} µPPN.")


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
        """, (user_id, new_balance))

        conn.commit()
    except ValueError as e:
        print(f"Error in update_user_balance: {e}")
        raise e



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
        if symbol != "BlueChipOG":
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


def has_role(user, role_id):
    """Check if the user has a specific role."""
    return any(role.id == role_id for role in user.roles)


def get_tax_percentage(quantity: int, cost: Decimal) -> float:
    base_tax = 0.025
    random_factor = random.uniform(0.01115, 0.03654)  # Randomly fluctuate tax by 1-5%

    # Time-based variability
    current_month = datetime.now().month
    if current_month in [8, 11, 12]:  # For November and December, assume a tax discount for holidays
        seasonal_discount = -0.05
    else:
        seasonal_discount = 0

    # Check if today is Monday
    if datetime.now().weekday() == 0:  # Monday is 0, Tuesday is 1, etc.
        max_tax_rate = 0.15  # Set maximum tax to 10% on Mondays
    else:
        # Formulaic approach for max_tax_rate based on logarithmic progression
        max_tax_rate = 0.45 / (1 + math.exp(float(cost) / -500000000))

    # Formulaic approach (based on logarithmic progression)
    quantity_multiplier = 0.001 * (quantity ** 0.5)
    cost_multiplier = 0.001 * (float(cost) / 1000) ** 0.5

    tax_rate = base_tax + quantity_multiplier + cost_multiplier + random_factor + seasonal_discount

    # Limit the tax rate to the maximum tax on Mondays
    tax_rate = min(tax_rate, max_tax_rate)

    # Clipping the tax_rate between 0.01 (1%) to 0.5 (50%) to ensure it's not too low or too high
    tax_rate = max(0.01, min(tax_rate, 0.5))

    return tax_rate





# End Economy Engine

async def update_market_etf_price(bot, conn):
    guild_id = 1087147399371292732 # Hardcoded guild ID
    channel_id = 1161706930981589094  # Hardcoded channel ID
    guild = ctx.bot.get_guild(GUILD_ID)
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



##Black Jack Logic

class CurrencySystem(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.ctx = None
        self.last_claimed = {}
        self.short_targets = {}
        self.conn = setup_database()
        self.lock = asyncio.Lock()
        self.claimed_users = set()  # Set to store claimed user IDs
        self.update_topstocks.start()
        self.current_prices_stocks = {}  # Dictionary to store current prices for stocks
        self.old_prices_stocks = {}  # Dictionary to store old prices for stocks
        self.current_prices_etfs = {}  # Dictionary to store current values for ETFs
        self.old_prices_etfs = {}  # Dictionary to store old values for ETFs
        self.connection_pool = sqlite3.connect("currency_system.db", check_same_thread=False)
        self.last_job_times = {}
        self.games = {}


    def setup(self, ctx):
        self.ctx = ctx  # Set the context when setting up the Cog

    @contextmanager
    def get_cursor(self):
        connection = sqlite3.connect("currency_system.db")
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            connection.commit()
            connection.close()

    def get_connection(self) -> Connection:
        return self.connection_pool


    def close_connection(self, commit=True):
        if commit:
            self.connection_pool.commit()
        self.get_connection().close()





    @tasks.loop(minutes=1)
    async def update_topstocks(self):
        try:
            channel_id = 1161735935944306808 # Replace with your actual channel ID
            message_id = 1161736160998072400  # Replace with your actual message ID
            channel = self.bot.get_channel(channel_id)

            if channel:
                cursor = self.conn.cursor()

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
                    embed.add_field(name=f"High #{i}: {symbol}", value=f"Price: {price:,.2f} µPPN\nAvailable: {available:,}", inline=False)

                # Add fields for the top 5 lowest price stocks
                for i, (symbol, price, available) in enumerate(top_low_stocks, start=1):
                    embed.add_field(name=f"Low #{i}: {symbol}", value=f"Price: {price:,.2f} µPPN\nAvailable: {available:,}", inline=False)

                # Find and edit the existing message by ID
                existing_message = await channel.fetch_message(message_id)
                if existing_message:
                    try:
                        await existing_message.edit(embed=embed)
                    except discord.errors.NotFound:
                        # If the message is not found, handle accordingly
                        print("Message not found.")
                else:
                    # If the message is not found, handle accordingly
                    print("Message not found.")

        except sqlite3.Error as e:
            # Log error message for debugging
            print(f"Database error: {e}")

        except Exception as e:
            # Log error message for debugging
            print(f"An unexpected error occurred: {e}")

    def get_user_stock_amount(self, user_id, stock_name):
        cursor = self.conn.cursor()
        cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (user_id, stock_name))
        result = cursor.fetchone()
        return int(result[0]) if result else 0

    def get_available_stocks(self):
        conn = sqlite3.connect("currency_system.db")
        cursor = conn.cursor()
        cursor.execute("SELECT symbol FROM stocks")
        stocks = cursor.fetchall()
        conn.close()
        return [stock[0] for stock in stocks]

    async def execute_buy_order(self, user_id, stock_name, amount, price):
        cursor = self.conn.cursor()

        current_balance = self.get_user_balance(user_id)
        total_cost = price * Decimal(amount)
        tax_percentage = self.get_tax_percentage(amount, total_cost)
        fee = total_cost * Decimal(tax_percentage)
        total_cost_with_tax = total_cost + fee

        if total_cost_with_tax > current_balance:
            return False  # Insufficient funds

        new_balance = current_balance - total_cost_with_tax

        try:
            self.update_user_balance(user_id, new_balance)
        except ValueError:
            return False  # Error updating user balance

        cursor.execute("""
            INSERT INTO user_stocks (user_id, symbol, amount)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, symbol) DO UPDATE SET amount = amount + ?
        """, (user_id, stock_name, amount, amount))

        cursor.execute("""
            UPDATE stocks
            SET available = available - ?
            WHERE symbol = ?
        """, (amount, stock_name))

        # Record the transaction
        await self.log_transaction("Buy Stock", user_id, stock_name, amount, total_cost, total_cost_with_tax, current_balance, new_balance, price)

        self.conn.commit()
        return True

    async def execute_sell_order(self, user_id, stock_name, amount, price):
        cursor = self.conn.cursor()

        current_stock_amount = self.get_user_stock_amount(user_id, stock_name)
        if current_stock_amount < amount:
            return False  # Insufficient stocks

        earnings = price * Decimal(amount)
        tax_percentage = self.get_tax_percentage(amount, earnings)
        fee = earnings * Decimal(tax_percentage)
        total_earnings = earnings - fee

        current_balance = self.get_user_balance(user_id)
        new_balance = current_balance + total_earnings

        try:
            self.update_user_balance(user_id, new_balance)
        except ValueError:
            return False  # Error updating user balance

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

        # Record the transaction
        await self.log_transaction("Sell Stock", user_id, stock_name, amount, earnings, total_earnings, current_balance, new_balance, price)

        self.conn.commit()
        return True

    @commands.command(name='servers', help='Check how many servers the bot is in.')
    async def servers(self, ctx):
        server_list = "\n".join([guild.name for guild in self.bot.guilds])
        server_count = len(self.bot.guilds)

        if server_count == 0:
            await ctx.send("I am not in any servers.")
        else:
            await ctx.send(f"I am in {server_count} server(s):\n{server_list}")


#Game Help

    @commands.command(name='game-help', aliases=["help"], help='Display available commands for Discord µPPN.')
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

        addr_help = """
    Store P3 Address
    ```!store_addr```

    View P3 Addr
    ```!my_addr```

    View Stats
    ```!stats```

    View Stock Chart
    ```!chart BlueChipOG```
    ```!chart P3:BANK```
    """


        embeds = [
            Embed(title="Stocks Related Commands", description=stocks_help, color=0x0099FF),
            Embed(title="ETF Related Commands", description=etfs_help, color=0x00FF00),
            Embed(title="Address and General Help", description=addr_help, color=0x00FF00),
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




# Bot to Channel
    @commands.command(name="update", help="Reload the current cog.")
    async def update_cog(self, ctx):
        try:
            await self.bot.reload_extension("currency_system_cog")  # Replace "your_cog_module" with the actual module name of your cog
            await ctx.send("Stock Market Bot successfully updated.")
        except commands.ExtensionError as e:
            await ctx.send(f"An error occurred while updating the cog: {e}")



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


# Metrics

    @commands.command(name="top_wealth", help="Show the top 10 wealthiest users.")
    async def top_wealth(self, ctx):
        cursor = self.conn.cursor()
        P3addrConn = sqlite3.connect("P3addr.db")
        P3addrCursor = P3addrConn.cursor()

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
            vanity_address = get_vanity_address(P3addrConn, user_id)
            P3Addr = vanity_address if vanity_address else generate_crypto_address(user_id)

            # Format wealth in shorthand
            wealth_shorthand = self.format_value(total_wealth)

            embed.add_field(name=f"P3 Address", value=f"{P3Addr}\n({wealth_shorthand})", inline=False)

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
            return f"{value / 10 ** 15:,.2f}Q"
        elif value >= 10 ** 12:
            return f"{value / 10 ** 12:,.2f}T"
        elif value >= 10 ** 9:
            return f"{value / 10 ** 9:,.2f}B"
        elif value >= 10 ** 6:
            return f"{value / 10 ** 6:,.2f}M"
        else:
            return f"{value:,.2f}"
## End Wealth Functions



    @commands.command(name='etf_metric', help='Show the current value and top holders of an ETF.')
    async def etf_metric(self, ctx, etf_id: int):
        try:
            cursor = self.conn.cursor()

            # Get the current value of the ETF
            cursor.execute("SELECT value FROM etfs WHERE etf_id = ?", (etf_id,))
            result = cursor.fetchone()
            if result:
                etf_value = result[0]
                await ctx.send(f"Current value of ETF with ID {etf_id}: {etf_value} µPPN")
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
                top_holders_str = "\n".join([f"{generate_crypto_address(user_id)} - {quantity} shares" for user_id, quantity in top_holders])
                await ctx.send(f"Top holders of ETF with ID {etf_id}:\n{top_holders_str}")
            else:
                await ctx.send(f"No one currently holds shares in ETF with ID {etf_id}.")

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while fetching ETF information: {e}")


# Currency Tools


    @commands.command(name="give", help="Give a specified amount of µPPN to another user.")
    async def give_addr(self, ctx, target, amount: int):
        sender_id = ctx.author.id
        P3addrConn = sqlite3.connect("P3addr.db")


        if target.startswith("P3:"):
            p3_address = target
            user_id = get_user_id(P3addrConn, p3_address)
            if not user_id:
                await ctx.send("Invalid or unknown P3 address.")
                return
        else:
            await ctx.send("Please provide a valid P3 address.")
            return

        if amount <= 0:
            await ctx.send("The amount to give should be greater than 0.")
            return

        sender_balance = get_user_balance(self.conn, sender_id)

        if sender_balance < amount:
            await ctx.send(f"{ctx.author.mention}, you don't have enough µPPN to give. Your current balance is {sender_balance:,.2f} µPPN.")
            return

        # Deduct the amount from the sender's balance
        update_user_balance(self.conn, sender_id, sender_balance - amount)

        # Add the amount to the recipient's balance
        recipient_balance = get_user_balance(self.conn, user_id)
        update_user_balance(self.conn, user_id, recipient_balance + amount)

        # Log the transfer
        await log_transfer(ledger_conn, ctx, ctx.author.name, target, user_id, amount)

        await ctx.send(f"{ctx.author.mention}, you have successfully given {amount:,.2f} µPPN to {target}.")




    @commands.command(name="daily", help="Claim your daily µPPN.")
    async def daily(self, ctx):
        async with self.lock:  # Use the asynchronous lock
            user_id = ctx.author.id
            member = ctx.guild.get_member(user_id)
            current_time = datetime.now()

            # If the user hasn't claimed or 24 hours have passed since the last claim
            if user_id not in self.last_claimed or (current_time - self.last_claimed[user_id]).total_seconds() > 86400:
                if has_role(member, bronze_pass):
                    amount = random.randint(dailyMin * 2, dailyMax * 2)
                else:
                    amount = random.randint(dailyMin, dailyMax)
                current_balance = get_user_balance(self.conn, user_id)
                new_balance = current_balance + amount
                update_user_balance(self.conn, user_id, new_balance)
                await ctx.send(f"{ctx.author.mention}, you have claimed {amount:,.2f} µPPN. Your new balance is: {new_balance:,.2f} µPPN.")
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
        P3Addr = generate_crypto_address(ctx.author.id)
        balance = get_user_balance(self.conn, user_id)

        # Format balance with commas and make it more visually appealing
        formatted_balance = "{:,}".format(balance)
        formatted_balance = f"**{formatted_balance}** µPPN"

        embed = discord.Embed(
            title="Balance",
            description=f"Balance for {P3Addr}:",
            color=discord.Color.blue()
        )
        embed.set_thumbnail(url="https://mirror.xyz/_next/image?url=https%3A%2F%2Fimages.mirror-media.xyz%2Fpublication-images%2F8XKxIUMy9CE8zg54-ZsP3.png&w=828&q=75")  # Add your own coin icon URL
        embed.add_field(name="µPPN", value=formatted_balance, inline=False)
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
        await ctx.send(f"{ctx.author.mention}, you have added {amount:,.2f} µPPN. Your new balance is: {new_balance:,.2f} µPPN.")




# Debug Start

    @commands.command(name="check_addr", help="Check the financial stats of users associated with the specified address.")
    async def check_addr(self, ctx, target_address):
        P3addrConn = sqlite3.connect("P3addr.db")

        # Get the user_id associated with the target address
        user_id = get_user_id(P3addrConn, target_address)

        if not user_id:
            await ctx.send("Invalid or unknown P3 address.")
            return

        conn = sqlite3.connect('currency_system.db')
        cursor = conn.cursor()

        # Reuse the logic from stats command to get user financial stats
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
            embed = Embed(title=f"{target_address} Financial Stats", color=Colour.green())
            embed.add_field(name="Balance", value=f"{current_balance:,.0f} µPPN", inline=False)
            embed.add_field(name="Total Stock Value", value=f"{total_stock_value:,.0f} µPPN", inline=False)
            embed.add_field(name="Total ETF Value", value=f"{total_etf_value:,.0f} µPPN", inline=False)
            embed.add_field(name="Total Funds Value", value=f"{total_funds_value:,.0f} µPPN", inline=False)

            await ctx.send(embed=embed)

        except sqlite3.Error as e:
            # Log error message for debugging
            print(f"Database error: {e}")

            # Inform the user that an error occurred
            await ctx.send(f"An error occurred while checking the financial stats. Please try again later.")

        except Exception as e:
            # Log error message for debugging
            print(f"An unexpected error occurred: {e}")

            # Inform the user that an error occurred
            await ctx.send(f"An unexpected error occurred. Please try again later.")

    @commands.command(name="check_addr_stats", help="Check the financial and inventory stats of the specified address.")
    async def check_addr_stats(self, ctx, target_address):
        P3addrConn = sqlite3.connect("P3addr.db")

        # Get the user_id associated with the target address
        user_id = get_user_id(P3addrConn, target_address)

        if not user_id:
            await ctx.send("Invalid or unknown P3 address.")
            return

        conn = sqlite3.connect('currency_system.db')
        cursor = conn.cursor()

        # Reuse the logic to get user financial stats
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

            # Fetch user's inventory
            cursor.execute("""
                SELECT items.item_name, items.item_description, inventory.quantity, items.price
                FROM inventory
                JOIN items ON inventory.item_id = items.item_id
                WHERE user_id=?
            """, (user_id,))
            inventory_data = cursor.fetchall()

            # Create the financial stats embed
            financial_stats_embed = discord.Embed(title=f"{target_address} Financial Stats", color=discord.Color.green())
            financial_stats_embed.add_field(name="Balance", value=f"{current_balance:,.0f} µPPN", inline=False)
            financial_stats_embed.add_field(name="Total Stock Value", value=f"{total_stock_value:,.0f} µPPN", inline=False)
            financial_stats_embed.add_field(name="Total ETF Value", value=f"{total_etf_value:,.0f} µPPN", inline=False)
            financial_stats_embed.add_field(name="Total Funds Value", value=f"{total_funds_value:,.0f} µPPN", inline=False)

            # Create the inventory embed
            inventory_embed = discord.Embed(title="Inventory", color=discord.Color.blue())

            total_value = 0  # Initialize total value

            for item in inventory_data:
                item_name = item[0]
                item_description = item[1] or "No description available"
                quantity = item[2]
                item_price = Decimal(item[3])  # Convert the item price to Decimal

                # Calculate the total value for the item
                item_value = item_price * quantity

                total_value += item_value  # Accumulate the total value

                # Format the values with commas
                formatted_quantity = "{:,}".format(quantity)
                formatted_item_value = "{:,.2f}".format(item_value)

                inventory_embed.add_field(name=item_name, value=f"Description: {item_description}\nQuantity: {formatted_quantity}\nValue: {formatted_item_value} µPPN", inline=False)

            # Format the total inventory value with commas
            formatted_total_value = "{:,.2f}".format(total_value)

            # Add the total value of the inventory to the embed
            inventory_embed.add_field(name="Total Inventory Value", value=f"{formatted_total_value} µPPN", inline=False)

            # Send the financial stats embed
            await ctx.send(embed=financial_stats_embed)

            # Send the inventory embed
            await ctx.send(embed=inventory_embed)


        except sqlite3.Error as e:
            # Log error message for debugging
            print(f"Database error: {e}")

            # Inform the user that an error occurred
            await ctx.send(f"An error occurred while checking the financial and inventory stats. Please try again later.")

        except Exception as e:
            # Log error message for debugging
            print(f"An unexpected error occurred: {e}")

            # Inform the user that an unexpected error occurred
            await ctx.send(f"An unexpected error occurred. Please try again later.")
        finally:
            # Close the database connections
            P3addrConn.close()
            conn.close()


    @commands.command(name="addr_metric", help="Show metrics for a P3 address.")
    async def addr_metric(self, ctx, target_address):
        # Connect to P3 address database
        P3addrConn = sqlite3.connect("P3addr.db")

        # Get user_id associated with the target address
        user_id = get_user_id(P3addrConn, target_address)

        if not user_id:
            await ctx.send("Invalid or unknown P3 address.")
            return

        # Connect to ledger database
        ledger_conn = sqlite3.connect("p3ledger.db")
        ledger_cursor = ledger_conn.cursor()

        # Connect to currency system database
        currency_conn = sqlite3.connect("currency_system.db")
        currency_cursor = currency_conn.cursor()

        try:
            # Get total buy and sell value and amounts of stocks and ETFs
            ledger_cursor.execute("""
                SELECT action, symbol, SUM(quantity) AS total_quantity, SUM(pre_tax_amount) AS total_pre_tax_amount
                FROM stock_transactions
                WHERE user_id=?
                GROUP BY action, symbol
            """, (user_id,))
            stock_metrics = ledger_cursor.fetchall()

            ledger_cursor.execute("""
                SELECT action, symbol, SUM(quantity) AS total_quantity, SUM(pre_tax_amount) AS total_pre_tax_amount
                FROM stock_transactions
                WHERE user_id=?
                GROUP BY action, symbol
            """, (user_id,))
            etf_metrics = ledger_cursor.fetchall()

            # Create an embed for displaying metrics
            embed = discord.Embed(title=f"Metrics for {target_address}", color=discord.Color.blue())

            # Display stock metrics
            stock_pages = create_multipage_embeds(stock_metrics, "Stock Metrics")
            for page in stock_pages:
                await ctx.send(embed=page)

            # Display ETF metrics
            etf_pages = create_multipage_embeds(etf_metrics, "ETF Metrics")
            for page in etf_pages:
                await ctx.send(embed=page)

        except sqlite3.Error as e:
            # Log error message for debugging
            print(f"Database error: {e}")

            # Inform the user that an error occurred
            await ctx.send(f"An error occurred while fetching metrics. Please try again later.")

        except Exception as e:
            # Log error message for debugging
            print(f"An unexpected error occurred: {e}")

            # Inform the user that an unexpected error occurred
            await ctx.send(f"An unexpected error occurred. Please try again later.")

        finally:
            # Close the database connections
            P3addrConn.close()
            ledger_conn.close()
            currency_conn.close()








    @commands.command(name="stock_stats", help="Get total buys, total sells, average price, current price, and circulating supply of a stock.")
    async def stock_stats(self, ctx, symbol: str):
        # Retrieve relevant transactions for the specified stock symbol from the ledger database
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
        etfs = set()

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

        # Connect to currency_system.db and fetch the current price and ETF information
        currency_conn = sqlite3.connect("currency_system.db")
        cursor_currency = currency_conn.cursor()

        # Fetch current price and available supply from stocks table
        cursor_currency.execute("SELECT available, price FROM stocks WHERE symbol = ?", (symbol,))
        result = cursor_currency.fetchone()
        # Fetch ETFs containing the specified stock
        cursor_currency.execute("""
            SELECT DISTINCT etfs.etf_id
            FROM etf_stocks
            INNER JOIN etfs ON etf_stocks.etf_id = etfs.etf_id
            WHERE etf_stocks.symbol = ?;
        """, (symbol,))
        etfs_containing_stock = [row[0] for row in cursor_currency.fetchall()]


        if result:
            available_supply, current_price = result
            formatted_current_price = '{:,.2f}'.format(current_price)
        else:
            await ctx.send(f"{symbol} is not a valid stock symbol.")
            return

        # Fetch circulating supply from user_stocks table
        cursor_currency = currency_conn.cursor()
        cursor_currency.execute("SELECT SUM(amount) FROM user_stocks WHERE symbol = ?", (symbol,))
        circulating_supply = cursor_currency.fetchone()[0] or 0
        currency_conn.close()

        # Determine if it's over or undervalued
        valuation_label = "Overvalued🔴" if current_price > average_price else "Undervalued🟢"

        # Format totals with commas
        formatted_total_buys = '{:,}'.format(int(total_buys))
        formatted_total_sells = '{:,}'.format(int(total_sells))

        # Create an embed to display the statistics
        embed = discord.Embed(
            title=f"Stock Statistics for {symbol}📊",
            color=discord.Color.blue()
        )
        circulatingValue = circulating_supply * current_price
        formatet_circulating_value = '{:,}'.format(int(circulatingValue))
        embed.add_field(name="Total Buys💵", value=f"{formatted_total_buys} µPPN ({total_quantity_buys:,} buys📈)", inline=False)
        embed.add_field(name="Total Sells💵", value=f"{formatted_total_sells} µPPN ({total_quantity_sells:,} sells📉)", inline=False)
        embed.add_field(name="Average Price", value=f"{average_price:,.2f} µPPN🔁", inline=False)
        embed.add_field(name="Current Price", value=f"{formatted_current_price} µPPN", inline=False)
        embed.add_field(name="Circulating Supply", value=f"{circulating_supply:,} shares🔄", inline=False)
        embed.add_field(name="Circulating Value", value=f"{formatet_circulating_value} µPPN")
        embed.add_field(name="Valuation", value=valuation_label, inline=False)
        if etfs_containing_stock:
            embed.add_field(name="ETFs Containing Stock", value=", ".join(map(str, etfs_containing_stock)), inline=False)


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
                embed.add_field(name="Total Buys", value=f"{formatted_total_buys} µPPN ({total_quantity_buys:,} buys)", inline=False)
                embed.add_field(name="Total Sells", value=f"{formatted_total_sells} µPPN ({total_quantity_sells:,} sells)", inline=False)
                embed.add_field(name="Average Price", value=f"{formatted_average_price} µPPN", inline=False)
                embed.add_field(name="Current ETF Value", value=f"{formatted_etf_value} µPPN", inline=False)
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

            # Calculate the total market volume
            total_market_volume = sum(stock[1] * stock[2] for stock in stocks)
            formatted_total_market_volume = '{:,.2f}'.format(total_market_volume)

            # Calculate the top 5 undervalued stocks
            currency_cursor.execute("SELECT symbol, price, available FROM stocks ORDER BY price / (price + 1) LIMIT 5")
            undervalued_stocks = currency_cursor.fetchall()

            # Calculate the top 5 overvalued stocks
            currency_cursor.execute("SELECT symbol, price, available FROM stocks ORDER BY price / (price - 1) LIMIT 5")
            overvalued_stocks = currency_cursor.fetchall()

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

            # Calculate the total market cap
            total_market_cap = total_market_value + total_etf_value
            formatted_total_market_cap = '{:,.2f}'.format(total_market_cap)

            # Calculate community profits/losses for all stocks and ETFs
            ledger_cursor.execute("""
                SELECT action, SUM(quantity), SUM(price)
                FROM stock_transactions
                WHERE action LIKE 'Buy%' OR action LIKE 'Sell%'
                GROUP BY action
            """)
            community_transactions_data = ledger_cursor.fetchall()

            total_community_buys = total_community_sells = total_community_profits_losses = 0

            for action, quantity, total_price in community_transactions_data:
                if "Buy" in action:
                    total_community_buys += total_price
                elif "Sell" in action:
                    total_community_sells += total_price

            total_community_profits_losses = total_community_sells - total_community_buys


            # Calculate most and least bought stocks
            ledger_cursor.execute("""
                SELECT symbol, SUM(quantity) AS total_quantity
                FROM stock_transactions
                WHERE action LIKE 'Buy%'
                GROUP BY symbol
                ORDER BY total_quantity DESC
                LIMIT 1
            """)
            most_bought_stock_data = ledger_cursor.fetchone()

            ledger_cursor.execute("""
                SELECT symbol, SUM(quantity) AS total_quantity
                FROM stock_transactions
                WHERE action LIKE 'Buy%'
                GROUP BY symbol
                ORDER BY total_quantity ASC
                LIMIT 1
            """)
            least_bought_stock_data = ledger_cursor.fetchone()

            most_bought_stock = most_bought_stock_data[0] if most_bought_stock_data else "None"
            least_bought_stock = least_bought_stock_data[0] if least_bought_stock_data else "None"

            # Calculate most and least bought ETFs
            ledger_cursor.execute("""
                SELECT symbol, SUM(quantity) AS total_quantity
                FROM stock_transactions
                WHERE action LIKE 'Buy ETF%'
                GROUP BY symbol
                ORDER BY total_quantity DESC
                LIMIT 1
            """)
            most_bought_etf_data = ledger_cursor.fetchone()

            ledger_cursor.execute("""
                SELECT symbol, SUM(quantity) AS total_quantity
                FROM stock_transactions
                WHERE action LIKE 'Buy ETF%'
                GROUP BY symbol
                ORDER BY total_quantity ASC
                LIMIT 1
            """)
            least_bought_etf_data = ledger_cursor.fetchone()

            most_bought_etf = most_bought_etf_data[0] if most_bought_etf_data else "None"
            least_bought_etf = least_bought_etf_data[0] if least_bought_etf_data else "None"

            # Format the values with commas and create an embed
            formatted_total_etf_value = '{:,.2f}'.format(total_etf_value)
            formatted_total_stock_value = '{:,.2f}'.format(total_stock_value)
            formatted_undervalued_stocks = "\n".join([f"{stock[0]}: {'{:,.2f}'.format(stock[1])} µPPN" for stock in undervalued_stocks])
            formatted_overvalued_stocks = "\n".join([f"{stock[0]}: {'{:,.2f}'.format(stock[1])} µPPN" for stock in overvalued_stocks])
            formatted_best_etf_id = best_etf_id if best_etf_id else "None"
            formatted_best_etf_value = '{:,.2f}'.format(best_etf_value)
            formatted_total_community_buys = '{:,.2f}'.format(total_community_buys)
            formatted_total_community_sells = '{:,.2f}'.format(total_community_sells)
            formatted_total_community_profits_losses = '{:,.2f}'.format(total_community_profits_losses)
            formatted_most_bought_stock = most_bought_stock
            formatted_least_bought_stock = least_bought_stock
            formatted_most_bought_etf = most_bought_etf
            formatted_least_bought_etf = least_bought_etf

            # Create an embed to display the market statistics
            embed = discord.Embed(
                title="Market Statistics",
                color=discord.Color.blue()
            )
            embed.add_field(name="Total ETF Value", value=f"{formatted_total_etf_value} µPPN", inline=False)
            embed.add_field(name="Total Market Cap", value=f"{formatted_total_market_cap} µPPN", inline=False)
            embed.add_field(name="Total Market Volume", value=f"{formatted_total_market_volume} shares", inline=False)
            embed.add_field(name="Top 5 Undervalued Stocks", value=formatted_undervalued_stocks, inline=False)
            embed.add_field(name="Top 5 Overvalued Stocks", value=formatted_overvalued_stocks, inline=False)
            embed.add_field(name="Best ETF to Buy", value=f"ETF {formatted_best_etf_id} ({best_etf_name}) with a value of {formatted_best_etf_value} µPPN", inline=False)
            embed.add_field(name="Community Total Buys", value=f"{formatted_total_community_buys} µPPN", inline=False)
            embed.add_field(name="Community Total Sells", value=f"{formatted_total_community_sells} µPPN", inline=False)
            embed.add_field(name="Community Total Profits/Losses", value=f"{formatted_total_community_profits_losses} µPPN", inline=False)
            embed.add_field(name="Most Bought Stock", value=f"{formatted_most_bought_stock}", inline=False)
            embed.add_field(name="Least Bought Stock", value=f"{formatted_least_bought_stock}", inline=False)
            embed.add_field(name="Most Bought ETF", value=f"{formatted_most_bought_etf}", inline=False)
            embed.add_field(name="Least Bought ETF", value=f"{formatted_least_bought_etf}", inline=False)

            # Send the embed as a message
            await ctx.send(embed=embed)

            # Close the database connections
            currency_conn.close()
            ledger_conn.close()

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred: {str(e)}")



    @commands.command(name="stock_chart", aliases=["chart"], help="Display a price history chart with technical indicators for a stock.")
    async def stock_chart(self, ctx, stock_symbol, time_period=None, rsi_period=None, sma_period=None, ema_period=None):
        try:
            # Connect to the currency_system database
            currency_conn = sqlite3.connect("currency_system.db")
            currency_cursor = currency_conn.cursor()

            # Check if the given stock symbol exists
            currency_cursor.execute("SELECT symbol, price FROM stocks WHERE symbol=?", (stock_symbol,))
            stock = currency_cursor.fetchone()

            if stock:
                stock_symbol, current_price = stock

                # Connect to the ledger database
                ledger_conn = sqlite3.connect("p3ledger.db")
                ledger_cursor = ledger_conn.cursor()

                # Retrieve buy/sell transactions for the stock from the ledger
                ledger_cursor.execute("""
                    SELECT timestamp, action, price
                    FROM stock_transactions
                    WHERE symbol=? AND (action='Buy Stock' OR action='Sell Stock')
                    ORDER BY timestamp
                """, (stock_symbol,))
                transactions = ledger_cursor.fetchall()

                if transactions:

            # Separate buy and sell transactions
                    buy_prices = []
                    sell_prices = []

                    for timestamp_str, action, price_str in transactions:
                        price = float(price_str)
                        datetime_obj = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")  # Parse timestamp string

                        if action == 'Buy Stock':
                            buy_prices.append((datetime_obj, price))
                        elif action == 'Sell Stock':
                            sell_prices.append((datetime_obj, price))

                    # Extract datetime objects and prices for buy and sell transactions
                    buy_timestamps = [entry[0] for entry in buy_prices]
                    buy_prices = [entry[1] for entry in buy_prices]
                    sell_timestamps = [entry[0] for entry in sell_prices]
                    sell_prices = [entry[1] for entry in sell_prices]
                    # Define a dictionary for time periods

                    time_periods = {
                        "1h": timedelta(hours=1),
                        "4h": timedelta(hours=4),
                        "1d": timedelta(days=1),
                        "2d": timedelta(days=2),
                        "3d": timedelta(days=3),
                        "4d": timedelta(days=4),
                        "5d": timedelta(days=5),
                        "1w": timedelta(weeks=1),
                        "2w": timedelta(weeks=2),
                        "3w": timedelta(weeks=3),
                        "4w": timedelta(weeks=4),
                        "1m": timedelta(weeks=4),
                        "2m": timedelta(weeks=8),
                        "6m": timedelta(weeks=24),
                        # Add more as needed
                    }


                    # Calculate the time period dynamically
#                    if time_period:
#                        try:
#                            time_delta = timedelta(**{time_period: 1})
#                        except TypeError:
#                            await ctx.send("Invalid time period. Please use a valid format like '1d', '2w', etc.")
#                            return
#                    else:
#                        # Default to 1 week if not provided
#                        time_delta = timedelta(weeks=24)

                    # Use default values if the user doesn't provide specific information
                    if not time_period or time_period.lower() == "none":
                        time_period = "6m"

                    if not rsi_period or rsi_period.lower() == "none":
                        rsi_period = 14

                    if not sma_period or sma_period.lower() == "none":
                        sma_period = 50

                    if not ema_period or ema_period.lower() == "none":
                        ema_period = 50


                    # Filter transactions based on the specified time period
                    start_date = datetime.now() - time_periods.get(time_period, timedelta(hours=1))
                    #start_date = datetime.now() - time_delta

                    buy_prices, buy_timestamps = zip(*[(p, t) for p, t in zip(buy_prices, buy_timestamps) if t >= start_date])
                    sell_prices, sell_timestamps = zip(*[(p, t) for p, t in zip(sell_prices, sell_timestamps) if t >= start_date])

                    # Calculate average buy and sell prices
                    average_buy_price = sum(buy_prices) / len(buy_prices) if buy_prices else 0
                    average_sell_price = sum(sell_prices) / len(sell_prices) if sell_prices else 0

                    # Determine if the stock is overvalued, undervalued, or fair-valued
                    if average_sell_price > 0:
                        price_ratio = average_buy_price / average_sell_price
                        if price_ratio > 1:
                            valuation = "Overvalued"
                        elif price_ratio < 1:
                            valuation = "Undervalued"
                        else:
                            valuation = "Fair-Valued"
                    else:
                        valuation = "Fair-Valued"

                    # Calculate RSI
                    if len(buy_prices) >= int(rsi_period):
                        rsi = talib.RSI(np.array(buy_prices), timeperiod=int(rsi_period))
                        rsi_timestamps = buy_timestamps[-len(rsi):]  # Match RSI timestamps
                    else:
                        rsi = None  # Insufficient data for RSI
                        rsi_timestamps = []

                    # Calculate Simple Moving Average (SMA)
                    if len(buy_prices) >= int(sma_period):
                        sma = talib.SMA(np.array(buy_prices), timeperiod=int(sma_period))
                        sma_timestamps = buy_timestamps[-len(sma):]  # Match SMA timestamps
                    else:
                        sma = None  # Insufficient data for SMA
                        sma_timestamps = []

                    # Calculate Exponential Moving Average (EMA)
                    if len(buy_prices) >= int(ema_period):
                        ema = talib.EMA(np.array(buy_prices), timeperiod=int(ema_period))
                        ema_timestamps = buy_timestamps[-len(ema):]  # Match EMA timestamps
                    else:
                        ema = None  # Insufficient data for EMA
                        ema_timestamps = []

                    # Calculate Bollinger Bands
                    bb_period = min(20, len(buy_prices))  # Adjust the period as needed
                    if len(buy_prices) >= bb_period:
                        upper, middle, lower = talib.BBANDS(np.array(buy_prices), timeperiod=bb_period)
                        bb_timestamps = buy_timestamps[-len(upper):]  # Match Bollinger Bands timestamps
                    else:
                        upper, middle, lower = None, None, None  # Insufficient data for Bollinger Bands
                        bb_timestamps = []

                    # Create a price history chart with all indicators and a separate RSI chart
                    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 10), gridspec_kw={'height_ratios': [3, 1]})

                    # Calculate average line
                    average_prices = [(t, (b + s) / 2) for t, b, s in zip(buy_timestamps, buy_prices, sell_prices)]
                    average_timestamps, average_prices = zip(*average_prices)

                    # Plot the average line on the price history chart
#                    ax1.plot(buy_timestamps, buy_prices, color='green', label='Price')
#                    ax1.plot(average_timestamps, average_prices, linestyle='-', color='black', label='Average Price')
                    if sma is not None and len(sma) == len(sma_timestamps):
                        ax1.plot(sma_timestamps, sma, color='purple', label=f'SMA ({sma_period}-period)')
                    if ema is not None and len(ema) == len(ema_timestamps):
                        ax1.plot(ema_timestamps, ema, color='blue', label=f'EMA ({ema_period}-period)')
                    if upper is not None and lower is not None:
                        ax1.fill_between(bb_timestamps, upper, lower, color='lightblue', alpha=0.5, label='Bollinger Bands')

                    ax1.set_title(f"Stock Chart for {stock_symbol}")
                    ax1.set_ylabel("Price")
                    ax1.grid(True)
                    ax1.legend()
                    ax1.tick_params(axis='x', rotation=45)
                    # Set y-axis limits on the price history chart
                    min_price = min(buy_prices + sell_prices)
                    max_price = max(buy_prices + sell_prices)
                    price_range = max_price - min_price
                    y_axis_margin = 0.001  # Add a margin to the y-axis limits
                    ax1.set_ylim(min_price - y_axis_margin * price_range, max_price + y_axis_margin * price_range)
                    if "h" in time_period:
                        ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
                    elif "d" in time_period:
                        ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
                    else:
                        ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
                    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))  # Format the date

                    # Plot RSI on the separate RSI chart
                    if rsi is not None and len(rsi) == len(rsi_timestamps):
                        ax2.plot(rsi_timestamps, rsi, color='orange', label=f'RSI ({rsi_period}-period)')
                        ax2.set_title(f"RSI Chart for {stock_symbol}")
                        ax2.set_ylabel("RSI")
                        ax2.grid(True)
                        ax2.legend()
                        if "h" in time_period:
                            ax2.xaxis.set_major_locator(mdates.DayLocator(interval=1))
                        elif "d" in time_period:
                            ax2.xaxis.set_major_locator(mdates.DayLocator(interval=1))
                        else:
                            ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
                        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))  # Format the date
                        ax2.tick_params(axis='x', rotation=45)


                    # Save the chart to a BytesIO object
                    buffer = io.BytesIO()
                    plt.savefig(buffer, format='png')
                    buffer.seek(0)

                    # Send the chart as a Discord message
                    file = discord.File(buffer, filename='chart.png')
                    await ctx.send(file=file)

                    # Create an embed with information
                    embed = discord.Embed(title=f"Stock Information for {stock_symbol}")
                    embed.add_field(name="Current Price", value=f"{current_price:,.2f}")
                    embed.add_field(name="Average Buy Price", value=f"{average_buy_price:,.2f}")
                    embed.add_field(name="Average Sell Price", value=f"{average_sell_price:,.2f}")
                    embed.add_field(name="Valuation", value=valuation)
                    if rsi is not None:
                        embed.add_field(name="RSI", value=f"{rsi[-1]:,.2f}")
                    if sma is not None:
                        embed.add_field(name="SMA", value=f"{sma[-1]:,.2f}")
                    if ema is not None:
                        embed.add_field(name="EMA", value=f"{ema[-1]:,.2f}")
                    embed.set_footer(text="Data may not be real-time")

                    # Send the embed as a Discord message
                    await ctx.send(embed=embed)
                    await self.stock_info(ctx, stock_symbol)
                    await self.stock_stats(ctx, stock_symbol)

                    # Close the ledger database connection
                    ledger_conn.close()

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred: {str(e)}")



    @commands.command(name="etf_chart", help="Display a price history chart with technical indicators for an ETF.")
    async def etf_chart(self, ctx, etf_symbol, time_period=None, rsi_period=None, sma_period=None, ema_period=None):
        try:
            # Connect to the currency_system database
            currency_conn = sqlite3.connect("currency_system.db")
            currency_cursor = currency_conn.cursor()

            current_price = await get_etf_value(currency_conn, etf_symbol)

            etf = etf_symbol, current_price


            if etf:
                etf_symbol, current_price = etf

                # Connect to the ledger database
                ledger_conn = sqlite3.connect("p3ledger.db")
                ledger_cursor = ledger_conn.cursor()

                # Retrieve ETF buy/sell transactions from the ledger
                ledger_cursor.execute("""
                    SELECT timestamp, action, price
                    FROM stock_transactions
                    WHERE symbol=? AND (action='Buy ETF' OR action='Sell ETF' OR action='Buy ALL ETF' OR action='Sell ALL ETF')
                    ORDER BY timestamp
                """, (etf_symbol,))
                transactions = ledger_cursor.fetchall()

                if transactions:
                    # Separate buy and sell transactions
                    buy_prices = []
                    sell_prices = []

                    for timestamp_str, action, price_str in transactions:
                        price = float(price_str)
                        datetime_obj = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")  # Parse timestamp string

                        if "Buy" in action:
                            buy_prices.append((datetime_obj, price))
                        elif "Sell" in action:
                            sell_prices.append((datetime_obj, price))

                    # Extract datetime objects and prices for buy and sell transactions
                    buy_timestamps = [entry[0] for entry in buy_prices]
                    buy_prices = [entry[1] for entry in buy_prices]
                    sell_timestamps = [entry[0] for entry in sell_prices]
                    sell_prices = [entry[1] for entry in sell_prices]

                    # Define a dictionary for time periods
                    time_periods = {
                        "1h": timedelta(hours=1),
                        "4h": timedelta(hours=4),
                        "1d": timedelta(days=1),
                        "2d": timedelta(days=2),
                        "3d": timedelta(days=3),
                        "4d": timedelta(days=4),
                        "5d": timedelta(days=5),
                        "1w": timedelta(weeks=1),
                        "2w": timedelta(weeks=2),
                        "3w": timedelta(weeks=3),
                        "4w": timedelta(weeks=4),
                        "1m": timedelta(weeks=4),
                        "2m": timedelta(weeks=8),
                        "6m": timedelta(weeks=24),
                        # Add more as needed
                        }

                    # Use default values if the user doesn't provide specific information
                    if not time_period or time_period.lower() == "none":
                        time_period = "6m"

                    if not rsi_period or rsi_period.lower() == "none":
                        rsi_period = 14

                    if not sma_period or sma_period.lower() == "none":
                        sma_period = 50

                    if not ema_period or ema_period.lower() == "none":
                        ema_period = 50

                    # Filter transactions based on the specified time period
                    start_date = datetime.now() - time_periods.get(time_period, timedelta(hours=1))
                    buy_prices, buy_timestamps = zip(*[(p, t) for p, t in zip(buy_prices, buy_timestamps) if t >= start_date])
                    sell_prices, sell_timestamps = zip(*[(p, t) for p, t in zip(sell_prices, sell_timestamps) if t >= start_date])

                    # Calculate average buy and sell prices
                    average_buy_price = sum(buy_prices) / len(buy_prices) if buy_prices else 0
                    average_sell_price = sum(sell_prices) / len(sell_prices) if sell_prices else 0

                    # Determine if the ETF is overvalued, undervalued, or fair-valued
                    if average_sell_price > 0:
                        price_ratio = average_buy_price / average_sell_price
                        if price_ratio > 1:
                            valuation = "Overvalued"
                        elif price_ratio < 1:
                            valuation = "Undervalued"
                        else:
                            valuation = "Fair-Valued"
                    else:
                        valuation = "Fair-Valued"

                    # Calculate RSI
                    if len(buy_prices) >= int(rsi_period):
                        rsi = talib.RSI(np.array(buy_prices), timeperiod=int(rsi_period))
                        rsi_timestamps = buy_timestamps[-len(rsi):]  # Match RSI timestamps
                    else:
                        rsi = None  # Insufficient data for RSI
                        rsi_timestamps = []

                    # Calculate Simple Moving Average (SMA)
                    if len(buy_prices) >= int(sma_period):
                        sma = talib.SMA(np.array(buy_prices), timeperiod=int(sma_period))
                        sma_timestamps = buy_timestamps[-len(sma):]  # Match SMA timestamps
                    else:
                        sma = None  # Insufficient data for SMA
                        sma_timestamps = []

                    # Calculate Exponential Moving Average (EMA)
                    if len(buy_prices) >= int(ema_period):
                        ema = talib.EMA(np.array(buy_prices), timeperiod=int(ema_period))
                        ema_timestamps = buy_timestamps[-len(ema):]  # Match EMA timestamps
                    else:
                        ema = None  # Insufficient data for EMA
                        ema_timestamps = []

                    # Calculate Bollinger Bands
                    bb_period = min(20, len(buy_prices))  # Adjust the period as needed
                    if len(buy_prices) >= bb_period:
                        upper, middle, lower = talib.BBANDS(np.array(buy_prices), timeperiod=bb_period)
                        bb_timestamps = buy_timestamps[-len(upper):]  # Match Bollinger Bands timestamps
                    else:
                        upper, middle, lower = None, None, None  # Insufficient data for Bollinger Bands
                        bb_timestamps = []

                    # Create a price history chart with all indicators and a separate RSI chart
                    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 10), gridspec_kw={'height_ratios': [3, 1]})

                    # Calculate average line
                    average_prices = [(t, (b + s) / 2) for t, b, s in zip(buy_timestamps, buy_prices, sell_prices)]
                    average_timestamps, average_prices = zip(*average_prices)

                    # Plot the average line on the price history chart
                    ax1.plot(buy_timestamps, buy_prices, linestyle='--', color='green', label='Price')
                    if sma is not None and len(sma) == len(sma_timestamps):
                        ax1.plot(sma_timestamps, sma, color='purple', label=f'SMA ({sma_period}-period)')
                    if ema is not None and len(ema) == len(ema_timestamps):
                        ax1.plot(ema_timestamps, ema, color='blue', label=f'EMA ({ema_period}-period)')
                    if upper is not None and lower is not None:
                        ax1.fill_between(bb_timestamps, upper, lower, color='lightblue', alpha=0.5, label='Bollinger Bands')

                    ax1.set_title(f"ETF Chart for {etf_symbol}")
                    ax1.set_ylabel("Price")
                    ax1.grid(True)
                    ax1.legend()
                    ax1.tick_params(axis='x', rotation=45)
                    # Set y-axis limits on the price history chart
                    min_price = min(buy_prices + sell_prices)
                    max_price = max(buy_prices + sell_prices)
                    price_range = max_price - min_price
                    y_axis_margin = 0.001  # Add a margin to the y-axis limits
                    ax1.set_ylim(min_price - y_axis_margin * price_range, max_price + y_axis_margin * price_range)
                    if "h" in time_period:
                        ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
                    elif "d" in time_period:
                        ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
                    else:
                        ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
                    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))  # Format the date

                    # Plot RSI on the separate RSI chart
                    if rsi is not None and len(rsi) == len(rsi_timestamps):
                        ax2.plot(rsi_timestamps, rsi, color='orange', label=f'RSI ({rsi_period}-period)')
                        ax2.set_title(f"RSI Chart for {etf_symbol}")
                        ax2.set_ylabel("RSI")
                        ax2.grid(True)
                        ax2.legend()
                        if "h" in time_period:
                            ax2.xaxis.set_major_locator(mdates.DayLocator(interval=1))
                        elif "d" in time_period:
                            ax2.xaxis.set_major_locator(mdates.DayLocator(interval=1))
                        else:
                            ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
                        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))  # Format the date
                        ax2.tick_params(axis='x', rotation=45)

                    # Save the chart to a BytesIO object
                    buffer = io.BytesIO()
                    plt.savefig(buffer, format='png')
                    buffer.seek(0)

                    # Send the chart as a Discord message
                    file = discord.File(buffer, filename='etf_chart.png')
                    await ctx.send(file=file)

                    # Create an embed with information
                    embed = discord.Embed(title=f"ETF Information for {etf_symbol}")
                    embed.add_field(name="Current Price", value=f"{current_price:,.2f}")
                    embed.add_field(name="Average Buy Price", value=f"{average_buy_price:,.2f}")
                    embed.add_field(name="Average Sell Price", value=f"{average_sell_price:,.2f}")
                    embed.add_field(name="Valuation", value=valuation)
                    if rsi is not None:
                        embed.add_field(name="RSI", value=f"{rsi[-1]:,.2f}")
                    if sma is not None:
                        embed.add_field(name="SMA", value=f"{sma[-1]:,.2f}")
                    if ema is not None:
                        embed.add_field(name="EMA", value=f"{ema[-1]:,.2f}")
                    embed.set_footer(text="Data may not be real-time")

                    # Send the embed as a Discord message
                    await ctx.send(embed=embed)

                    # Close the ledger database connection
                    ledger_conn.close()

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred: {str(e)}")


# Stock Market


# Buy Stock
    @commands.command(name="buy", aliases=["buy_stock"], help="Buy stocks. Provide the stock name and amount.")
    async def buy(self, ctx, stock_name: str, amount: int):
        buyer_id = ctx.author.id
        user_id = ctx.author.id
        member = ctx.guild.get_member(user_id)


        cursor = self.conn.cursor()
#        result, last_purchase_time, remaining_time = sell_check(cursor, user_id, stock_name, "30m")
#        if result:
#            print(f"You can not purchase {stock_name} for another {remaining_time}")
        if amount <= 0:
            await ctx.send("Amount must be a positive number.")
            return


        # Check if the stock_name is "TitanForge" and set a different limit
        if stock_name.lower() == "p3:stable":
            stock_limit = float('inf')  # Infinite limit for TitanForge
        else:
            if has_role(member, bronze_pass):
                stock_limit = dStockLimit * 1.5
            else:
                stock_limit = dStockLimit

        if amount == 0:
            await ctx.send(f"{ctx.author.mention}, you cannot buy 0 amount of {stock_name}.")
            return

        # Retrieve stock information
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
        stock = cursor.fetchone()

        if stock is None:
            await ctx.send(f"{ctx.author.mention}, this stock does not exist.")
            return

        price = Decimal(stock[2])  # Assuming stock[2] is the price

        # Get the total amount bought today by the user for this stock
        cursor.execute("""
            SELECT SUM(amount), MAX(timestamp)
            FROM user_daily_buys
            WHERE user_id=? AND symbol=? AND DATE(timestamp)=DATE('now')
        """, (buyer_id, stock_name))

        daily_bought_record = cursor.fetchone()
        daily_bought = daily_bought_record[0] if daily_bought_record and daily_bought_record[0] is not None else 0
        last_purchase_time = daily_bought_record[1] if daily_bought_record and daily_bought_record[1] is not None else None

        if user_id == jacob:
            stock_limit = float('inf')
        if daily_bought + amount > stock_limit:
            remaining_amount = stock_limit - daily_bought

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

        # Check if there are matching orders in the order book
        cursor.execute("SELECT user_id, price, quantity FROM limit_orders WHERE symbol=? ORDER BY price ASC, quantity ASC", (stock_name,))
        orders = cursor.fetchall()

        total_amount_bought = 0  # Track the total amount bought to check against the requested amount

        for order in orders:
            seller_id, order_price, order_quantity = order

            if order_price <= price:
                # Calculate the total cost for the order quantity
                total_cost_order = order_price * order_quantity

                if amount >= order_quantity:
                    # Execute the order from the order book

                    # Update buyer's balance
                    cursor.execute("SELECT balance FROM users WHERE user_id=?", (buyer_id,))
                    buyer_balance = cursor.fetchone()['balance']

                    total_cost = order_price * order_quantity  # Calculate total cost for the requested amount

                    if total_cost > buyer_balance:
                        await ctx.send(f"{ctx.author.mention}, you do not have sufficient balance to buy these stocks.")
                        return

                    # Update buyer's stocks
                    cursor.execute("""
                        INSERT INTO user_stocks (user_id, symbol, amount)
                        VALUES (?, ?, ?)
                        ON CONFLICT(user_id, symbol) DO UPDATE SET amount = amount + ?
                    """, (buyer_id, stock_name, order_quantity, order_quantity))

                    # Update seller's balance
                    cursor.execute("SELECT balance FROM users WHERE user_id=?", (seller_id,))
                    seller_balance = cursor.fetchone()['balance']
                    new_seller_balance = seller_balance + total_cost
                    update_user_balance(self.conn, seller_id, new_seller_balance)

                    # Update order book
                    new_quantity = order_quantity - amount
                    if new_quantity == 0:
                        cursor.execute("DELETE FROM limit_orders WHERE user_id=? AND symbol=? AND price=? AND quantity=?",
                                       (seller_id, stock_name, order_price, order_quantity))
                    else:
                        cursor.execute("UPDATE limit_orders SET quantity=? WHERE user_id=? AND symbol=? AND price=? AND quantity=?",
                                       (new_quantity, seller_id, stock_name, order_price, order_quantity))

                    total_amount_bought += order_quantity  # Update the total amount bought

                    self.conn.commit()

                    formatted_buyer_balance = f"{buyer_balance:,.2f}"
                    await log_transaction(ledger_conn, ctx, "Buy Order Book", stock_name, order_quantity, total_cost, total_cost, buyer_balance, formatted_buyer_balance, order_price)

                    # Notify the seller in DM
                    seller = self.bot.get_user(seller_id)
                    if seller:
                        try:
                            await seller.send(f"Your sell order for {order_quantity} stocks of '{stock_name}' at {order_price:,.2f} each has been filled. "
                                              f"Total: {total_cost:,.2f} µPPN. New balance: {new_seller_balance:,.2f} µPPN.")
                        except discord.errors.Forbidden:
                            print(f"Unable to send a message to the seller with ID {seller_id}. They may have blocked DMs or disabled them.")

                    await ctx.send(f"{ctx.author.mention}, you have bought {order_quantity} stocks of '{stock_name}' from the order book. Your new balance is: {formatted_buyer_balance} µPPN.")

                    amount -= order_quantity  # Subtract the bought quantity from the requested amount

                else:
                    # Execute a partial order from the order book

                    # Update buyer's balance
                    cursor.execute("SELECT balance FROM users WHERE user_id=?", (buyer_id,))
                    buyer_balance = cursor.fetchone()['balance']

                    total_cost = order_price * amount  # Calculate total cost for the requested amount

                    if total_cost > buyer_balance:
                        await ctx.send(f"{ctx.author.mention}, you do not have sufficient balance to buy these stocks.")
                        return

                    # Update buyer's stocks
                    cursor.execute("""
                        INSERT INTO user_stocks (user_id, symbol, amount)
                        VALUES (?, ?, ?)
                        ON CONFLICT(user_id, symbol) DO UPDATE SET amount = amount + ?
                    """, (buyer_id, stock_name, amount, amount))

                    # Update seller's balance
                    cursor.execute("SELECT balance FROM users WHERE user_id=?", (seller_id,))
                    seller_balance = cursor.fetchone()['balance']
                    new_seller_balance = seller_balance + total_cost
                    update_user_balance(self.conn, seller_id, new_seller_balance)

                    # Update order book
                    cursor.execute("DELETE FROM limit_orders WHERE user_id=? AND symbol=? AND price=? AND quantity=?",
                                   (seller_id, stock_name, order_price, order_quantity))

                    total_amount_bought += amount  # Update the total amount bought

                    self.conn.commit()

                    formatted_buyer_balance = f"{buyer_balance:,.2f}"
                    await log_transaction(ledger_conn, ctx, "Buy Order Book", stock_name, amount, total_cost, total_cost, buyer_balance, formatted_buyer_balance, order_price)

                    # Notify the seller in DM
                    seller = self.bot.get_user(seller_id)
                    if seller:
                        try:
                            await seller.send(f"Your sell order for {amount:,.2f} stocks of '{stock_name}' at {order_price:,.2f} each has been partially filled. "
                                              f"Total: {total_cost:,.2f} µPPN. New balance: {new_seller_balance:,.2f} µPPN.")
                        except discord.errors.Forbidden:
                            print(f"Unable to send a message to the seller with ID {seller_id}. They may have blocked DMs or disabled them.")

                    await ctx.send(f"{ctx.author.mention}, you have bought {amount:,.2f} stocks of '{stock_name}' from the order book. Your new balance is: {formatted_buyer_balance} µPPN.")

                    break  # Exit the loop after processing the order

        # Proceed with the market buy if there's still remaining amount to buy
        if amount > 0 and total_amount_bought == 0:
            cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
            stock = cursor.fetchone()
            if stock is None:
                await ctx.send(f"{ctx.author.mention}, this stock does not exist.")
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
        tax_percentage = 0.15
        fee = cost * Decimal(tax_percentage)
        total_cost = cost + fee

        current_balance = get_user_balance(self.conn, user_id)

        if total_cost > current_balance:
            # Calculate the missing amount needed to complete the transaction including tax.
            missing_amount = total_cost - current_balance
            await ctx.send(f"{ctx.author.mention}, you do not have enough µPPN to buy these stocks. You need {missing_amount:,.2f} more µPPN, including tax, to complete this purchase.")
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
        if stock_name == ('P3:Stable'):
            await self.stableManager(ctx, 'BUY', amount)
        else:
            await self.increase_price(ctx, stock_name, amount)

        await self.blueChipBooster(ctx, "BUY")
        stock_amount = amount
        print(f"Debug: {stock_name}, Amount {stock_amount}, Price {price}, Stock Limit {stock_limit}")
#        if (
#            stock_name.lower() == "p3:lqdy"
#            and price <= 1000000
#            and stock_amount <= stock_limit
#        ):
#            await self.reward_share_holders(ctx, "p3:lqdy")
        self.conn.commit()
        if amount == 0:
            print(f'Fix for 0 amount buy after order book')
        else:
            await log_transaction(ledger_conn, ctx, "Buy Stock", stock_name, amount, cost, total_cost, current_balance, new_balance, price)
            await ctx.send(f"{ctx.author.mention}, you have bought {amount} {stock_name} stocks. Your new balance is: {new_balance:,.2f} µPPN.")



# Sell Stock
    @commands.command(name="sell", aliases=["sell_stock"], help="Sell stocks. Provide the stock name and amount.")
    async def sell(self, ctx, stock_name: str, amount: int):
        user_id = ctx.author.id
        member = ctx.guild.get_member(user_id)

        if amount <= 0:
            await ctx.send("Amount must be a positive number.")
            return



        await ctx.message.delete()
        cursor = self.conn.cursor()


#        result, last_purchase_time, remaining_time = buy_check(cursor, user_id, stock_name, "30m")
#        if result:
#            await ctx.send(f"Must wait {remaining_time} after purchase to sell")
#            return
#        else:
#            print("Buy:False")

        # Check if the user recently bought stocks
        cursor.execute("SELECT timestamp FROM user_daily_buys WHERE user_id=? AND symbol=? ORDER BY timestamp DESC LIMIT 1", (user_id, stock_name))
        last_buy_timestamp = cursor.fetchone()

        if last_buy_timestamp:
            last_buy_timestamp = datetime.strptime(last_buy_timestamp[0], "%Y-%m-%d %H:%M:%S")
            current_time = datetime.utcnow()
            time_difference = current_time - last_buy_timestamp

            if time_difference.total_seconds() < 1800:  # 1800 seconds = 30 minutes
                remaining_time = timedelta(seconds=1800) - time_difference
                await ctx.send(f"{ctx.author.mention}, you cannot sell {stock_name} stocks within {remaining_time}.")
                return

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
        await check_impact(cursor, ctx, stock_name, float(amount))
        price = Decimal(stock[2])
        earnings = price * Decimal(amount)
        tax_percentage = get_tax_percentage(amount, earnings)  # Custom function to determine the tax percentage based on quantity and earnings
        if has_role(member, bronze_pass):
            tax_percentage -= role_discount
        fee = earnings * Decimal(tax_percentage)
        total_earnings = earnings - fee

        current_balance = get_user_balance(self.conn, user_id)
        new_balance = current_balance + total_earnings

        try:
            update_user_balance(self.conn, user_id, new_balance)
        except ValueError as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while updating the user balance. Error: {str(e)}")
            return

        # Check for buy orders in the order book
        cursor.execute("SELECT user_id, price, quantity FROM limit_orders WHERE symbol=? ORDER BY price DESC, quantity DESC", (stock_name,))
        orders = cursor.fetchall()

        for order in orders:
            buyer_id, order_price, order_quantity = order

            if order_price >= price:
                # Execute the order from the order book

                # Update seller's balance
                cursor.execute("SELECT balance FROM users WHERE user_id=?", (user_id,))
                seller_balance = cursor.fetchone()['balance']
                new_seller_balance = seller_balance + total_earnings
                update_user_balance(self.conn, user_id, new_seller_balance)

                # Update buyer's stocks
                cursor.execute("""
                    INSERT INTO user_stocks (user_id, symbol, amount)
                    VALUES (?, ?, ?)
                    ON CONFLICT(user_id, symbol) DO UPDATE SET amount = amount + ?
                """, (buyer_id, stock_name, order_quantity, order_quantity))

                # Update order book
                new_quantity = order_quantity - amount
                if new_quantity == 0:
                    cursor.execute("DELETE FROM limit_orders WHERE user_id=? AND symbol=? AND price=? AND quantity=?",
                                   (buyer_id, stock_name, order_price, order_quantity))
                else:
                    cursor.execute("UPDATE limit_orders SET quantity=? WHERE user_id=? AND symbol=? AND price=? AND quantity=?",
                                   (new_quantity, buyer_id, stock_name, order_price, order_quantity))

                self.conn.commit()

                formatted_seller_balance = f"{new_seller_balance:,.2f}"
                await log_transaction(ledger_conn, ctx, "Sell Order Book", stock_name, order_quantity, total_earnings, total_earnings, seller_balance, formatted_seller_balance, price)

                # Notify the buyer in DM
                buyer = self.bot.get_user(buyer_id)
                if buyer:
                    try:
                        await buyer.send(f"Your buy order for {order_quantity:,.2f} stocks of '{stock_name}' at {order_price:,.2f} each has been filled. "
                                         f"Total: {total_earnings:,.2f} µPPN. New balance: {formatted_seller_balance} µPPN.")
                    except discord.errors.Forbidden:
                        print(f"Unable to send a message to the buyer with ID {buyer_id}. They may have blocked DMs or disabled them.")

                await ctx.send(f"{ctx.author.mention}, you have sold {amount:,.2f} stocks of '{stock_name}' to a buy order from the order book. Your new balance is: {formatted_seller_balance} µPPN.")

                amount -= order_quantity  # Subtract the sold quantity from the requested amount

                if amount == 0:
                    # If the requested amount is fully sold, exit the loop
                    break

        # Proceed with the sell order if there's still remaining amount to sell
        if amount > 0:
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

            # Record this transaction in the user_daily_buys table
            try:
                cursor.execute("""
                    INSERT INTO user_daily_sells (user_id, symbol, amount, timestamp)
                    VALUES (?, ?, ?, datetime('now'))
                """, (user_id, stock_name, amount))
            except sqlite3.Error as e:
                await ctx.send(f"{ctx.author.mention}, an error occurred while updating daily stock limit. Error: {str(e)}")
                return

            if stock_name == ('P3:Stable'):
                await self.stableManager(ctx, 'sell', amount)
            else:
                await self.decrease_price(ctx, stock_name, amount)
            await log_transaction(ledger_conn, ctx, "Sell Stock", stock_name, amount, earnings, total_earnings, current_balance, new_balance, price)
            self.conn.commit()
            await self.blueChipBooster(ctx, "SELL")
            await self.P3LQDYBooster(ctx)
            await ctx.send(f"{ctx.author.mention}, you have sold {amount:,.2f} stocks of '{stock_name}'. Your new balance is: {new_balance:,.2f} µPPN.")




# Buy/Sell Multi




    @commands.command(name="buyMulti", aliases=["buy_multi"], help="Buy stocks for all stocks in the market. Provide the amount to buy.")
    async def buy_multi(self, ctx, amount: int):
        user_id = ctx.author.id
        current_time = datetime.now()
        total_spent = 0
        price_changes = {}

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
        await ctx.send(f'buyMulti has began with {len(market_stocks) + 1} Stocks at {amount} shares per stock')
        await ctx.send('A confirmation will display after command has completed')

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

            if user_id == jacob:
                StockLimit = float('inf')
            else:
                StockLimit = dStockLimit
            if daily_bought + amount > StockLimit:
                remaining_amount = StockLimit - daily_bought

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
                               f"You can buy {remaining_amount:,.2f} more after {time_remaining_str}.")
                continue

            if amount > available:
                await ctx.send(f"{ctx.author.mention}, there are only {available:,.2f} {stock_name} stocks available for purchase.")
                continue

            cost = price * Decimal(amount)
            tax_percentage = get_tax_percentage(amount, cost)
            fee = cost * Decimal(tax_percentage)
            total_cost = cost + fee

            current_balance = get_user_balance(self.conn, user_id)

            if total_cost > current_balance:
                # Calculate the missing amount needed to complete the transaction including tax.
                missing_amount = total_cost - current_balance
                await ctx.send(f"{ctx.author.mention}, you do not have enough µPPN to buy {amount:,.2f} {stock_name} stocks. "
                               f"You need {missing_amount:,.2f} more µPPN, including tax, to complete this purchase.")
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
            await self.blueChipBooster(ctx, "BUY")
            if stock_name == ('P3:Stable'):
                await self.stableManager(ctx, 'BUY', amount)
            else:
                await self.increase_price_non_verbose(ctx, stock_name, amount)
            await log_transaction(ledger_conn, ctx, "Buy Stock", stock_name, amount, cost, total_cost, current_balance, new_balance, price)

            # Update total spent
            total_spent += total_cost

            # Store price change for later display
            price_changes[stock_name] = {
                'old_price': price,
                'new_price': await get_stock_price(self.conn, stock_name)
            }

        # Send a summary embed with total spent and price changes
        await self.send_purchase_summary(ctx, total_spent, price_changes)
        await ctx.send(f'{ctx.author.mention}')

        last_buy_time[user_id] = current_time
        self.conn.commit()

    async def send_purchase_summary(self, ctx, total_spent, price_changes):
        # Create an embed with total spent and price changes
        embed = discord.Embed(title="Purchase Summary", color=0x00ff00)
        embed.add_field(name="Total Spent", value=f"{total_spent:,.2f} µPPN", inline=False)

        # List to store individual pages
        pages = []

        # Add fields for each stock's price change
        for stock_name, prices in price_changes.items():
            old_price = Decimal(prices['old_price'])
            new_price = Decimal(prices['new_price'])
            price_change = Decimal(new_price - old_price)
            percent_change = (price_change / old_price) * 100

            # Add information to the current page
            embed.add_field(name=f"{stock_name} Price Change", value=f"Old Price: {old_price:,.2f} µPPN\n"
                                                                     f"New Price: {new_price:,.2f} µPPN\n"
                                                                     f"Change: {price_change:,.2f} µPPN\n"
                                                                     f"Percent Change: {percent_change:,.2f}%", inline=False)

            # If the current embed size exceeds the maximum, start a new page
            if len(embed) > 5800:
                pages.append(embed)
                embed = discord.Embed(title="Purchase Summary", color=0x00ff00)
                embed.add_field(name="Total Spent", value=f"{total_spent:,.2f} µPPN", inline=False)

        # Add the last page to the list
        pages.append(embed)

        # Send pages one by one
        for page in pages:
            await ctx.send(embed=page)


    @commands.command(name="sellMulti", aliases=["sell_multi"], help="Sell stocks for all stocks in the market. Provide the amount to sell.")
    @is_allowed_user(930513222820331590)
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

            # Check if the user recently bought stocks
            cursor.execute("SELECT timestamp FROM user_daily_buys WHERE user_id=? AND symbol=? ORDER BY timestamp DESC LIMIT 1", (user_id, stock_name))
            last_buy_timestamp = cursor.fetchone()

            if last_buy_timestamp:
                last_buy_timestamp = datetime.strptime(last_buy_timestamp[0], "%Y-%m-%d %H:%M:%S")
                time_difference = current_time - last_buy_timestamp

                if time_difference.total_seconds() < 1800:  # 1800 seconds = 30 minutes
                    remaining_time_dump = timedelta(seconds=1800) - time_difference
                    await ctx.send(f"{ctx.author.mention}, you cannot sell {stock_name} stocks within {remaining_time_dump}.")
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

            if stock_name == ('P3:Stable'):
                await self.stableManager(ctx, 'sell', amount)
            else:
                await self.decrease_price(ctx, stock_name, amount)
            await self.blueChipBooster(ctx, "SELL")
            await log_transaction(ledger_conn, ctx, "Sell Stock", stock_name, amount, earnings, total_earnings, current_balance, new_balance, price)
            last_sell_time[user_id] = current_time
            self.conn.commit()



    @commands.command(name="buy_multi_stock", help="Buy multiple stocks from a specified ETF. Provide ETF ID and amount of shares.")
    @is_allowed_user(930513222820331590)
    async def buy_multi_stock(self, ctx, etf_id: int, amount: int):
        user_id = ctx.author.id
        cursor = self.conn.cursor()

        await ctx.message.delete()

        # Fetch stocks and their price and available supply in the specified ETF
        cursor.execute("""
            SELECT s.symbol, s.price, s.available
            FROM etf_stocks AS e
            INNER JOIN stocks AS s ON e.symbol = s.symbol
            WHERE e.etf_id = ?
        """, (etf_id,))
        stocks_info = cursor.fetchall()

        if not stocks_info:
            await ctx.send("Invalid ETF ID.")
            return

        total_cost = Decimal(0)

        for stock_info in stocks_info:
            stock_name, price, available_supply = stock_info

            # Get the total amount bought today by the user for this stock
            cursor.execute("""
                SELECT COALESCE(SUM(amount), 0) as total_amount
                FROM user_daily_buys
                WHERE user_id=? AND symbol=? AND DATE(timestamp)=DATE('now')
            """, (user_id, stock_name))

            daily_bought_record = cursor.fetchone()
            daily_bought = daily_bought_record["total_amount"]

            if daily_bought + amount > dStockLimit:
                remaining_amount = dStockLimit - daily_bought

                await ctx.send(f"{ctx.author.mention}, you have reached your daily buy limit for {stock_name} stocks. "
                               f"You can buy {remaining_amount} more.")
                return

            # Your existing buy logic
            available, _, _ = int(available_supply), Decimal(price), 0

            if amount > available:
                await ctx.send(f"{ctx.author.mention}, there are only {available} {stock_name} stocks available for purchase.")
                continue

            cost = Decimal(price) * Decimal(amount)
            tax_percentage = get_tax_percentage(amount, cost)
            fee = cost * Decimal(tax_percentage)
            total_cost += cost + fee

        # Calculate the tax amount based on dynamic factors
        tax_percentage_total = get_tax_percentage(amount, total_cost)
        fee_total = total_cost * Decimal(tax_percentage_total)
        total_cost_with_tax = total_cost + fee_total

        # Check if user has enough balance to buy the stocks
        current_balance = get_user_balance(self.conn, user_id)

        if total_cost_with_tax > current_balance:
            # Calculate the missing amount needed to complete the transaction including tax.
            missing_amount = total_cost - current_balance
            await ctx.send(f"{ctx.author.mention}, you do not have enough µPPN to buy these stocks. "
                           f"You need {missing_amount:,.2f} more µPPN, including tax, to complete this purchase.")
            return

        new_balance = current_balance - total_cost_with_tax

        # Update user's balance
        try:
            update_user_balance(self.conn, user_id, new_balance)
        except ValueError as e:
            await ctx.send(f"An error occurred while updating the user balance. Error: {str(e)}")
            return

        # Update user's stocks for each stock in the ETF
        for stock_info in stocks_info:
            stock_name, _, _ = stock_info
            try:
                cursor.execute("""
                    INSERT INTO user_stocks (user_id, symbol, amount)
                    VALUES (?, ?, ?)
                    ON CONFLICT(user_id, symbol) DO UPDATE SET amount = amount + ?
                """, (user_id, stock_name, amount, amount))
            except sqlite3.Error as e:
                await ctx.send(f"An error occurred while updating user stocks for {stock_name}. Error: {str(e)}")
                return

        # Update available stocks for each stock in the ETF
        for stock_info in stocks_info:
            stock_name, _, _ = stock_info
            try:
                cursor.execute("""
                    UPDATE stocks
                    SET available = available - ?
                    WHERE symbol = ?
                """, (amount, stock_name))
            except sqlite3.Error as e:
                await ctx.send(f"An error occurred while updating available stocks for {stock_name}. Error: {str(e)}")
                return

        # Record this transaction in the user_daily_buys table for each stock in the ETF
        for stock_info in stocks_info:
            stock_name, _, _ = stock_info
            try:
                cursor.execute("""
                    INSERT INTO user_daily_buys (user_id, symbol, amount, timestamp)
                    VALUES (?, ?, ?, datetime('now'))
                """, (user_id, stock_name, amount))
            except sqlite3.Error as e:
                await ctx.send(f"An error occurred while updating daily stock limit for {stock_name}. Error: {str(e)}")
                return

        # Record the team's transaction for each stock in the ETF
        cursor.execute("SELECT team_id FROM team_members WHERE user_id=?", (user_id,))
        team_record = cursor.fetchone()
        if team_record:
            team_id = team_record[0]
            for stock_info in stocks_info:
                stock_name, _, available_supply = stock_info
                # Record the team's transaction
                record_team_transaction(self.conn, team_id, stock_name, amount, available_supply, "buy")
            # Calculate and update the team's profit/loss
            calculate_team_profit_loss(self.conn, team_id)

        # Decay other stocks and log the overall transaction
        for stock_info in stocks_info:
            stock_name, stock_price, _ = stock_info
            decay_other_stocks(self.conn, stock_name)
            await self.increase_price(ctx, stock_name, amount)
            await self.blueChipBooster(ctx, "BUY")
            await log_transaction(ledger_conn, ctx, "Buy Stock", stock_name, amount, total_cost, total_cost_with_tax, current_balance, new_balance, stock_price)

        # Commit changes to the database
        self.conn.commit()

        await ctx.send(f"You have successfully bought {amount} units of stocks from ETF {etf_id}. Your new balance is: {new_balance:,.2f} µPPN.")


    @commands.command(name="as-user", help="Run a stock command as a specific user using their P3 address.")
    @is_allowed_user(930513222820331590)
    async def as_user(self, ctx, user_p3_address: str, *, command: str):
        conn = sqlite3.connect("P3addr.db")

        # Get the user ID from the P3 address
        user_id = get_user_id(conn, user_p3_address)

        if user_id is not None:
            # Create a new context with the target user as the author
            new_message = ctx.message
            new_message.author = ctx.guild.get_member(user_id)

            new_ctx = await self.bot.get_context(new_message)

            # Use shlex to properly parse the command and its arguments
            command_args = shlex.split(command)

            # Set the command and its arguments in the new context
            new_ctx.command = self.bot.get_command(command_args[0])
            new_ctx.message.content = ctx.prefix + command  # Update the content to include the prefix

            # Convert integer arguments or parameters, excluding the P3 address
            for i, arg in enumerate(command_args[1:]):
                # Skip the P3 address when converting to int
                if i == 0:
                    continue

                for param in new_ctx.command.clean_params.values():
                    if param.annotation == int and param.name in arg:
                        try:
                            command_args[i + 1] = int(arg)
                        except ValueError:
                            raise commands.BadArgument(f'Converting to "int" failed for parameter "{param.name}".')

            # Set the new arguments in the context
            new_ctx.args = tuple(command_args[1:])

            # Execute the specified command
            try:
                await self.bot.invoke(new_ctx)
                print(f"New context created with author: {new_ctx.author}")
                print(f"Command to invoke: {new_ctx.command}")

            except Exception as e:
                await ctx.send(f"An error occurred while running the command: {str(e)}")
        else:
            await ctx.send("User not found.")



# Limit Order

    @commands.command(name="limit_order", help="Place a limit order to buy or sell stocks.")
    @is_allowed_user(930513222820331590)
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
        if order_type.lower() not in ["sell"]:
            await ctx.send(f"{ctx.author.mention}, the order type must be 'sell'.")
            return

        # Check if the user has sufficient balance for a sell order
        if order_type.lower() == "sell":
            cursor.execute("SELECT balance FROM users WHERE user_id=?", (user_id,))
            result = cursor.fetchone()
            if not result or float(result["balance"]) < price * quantity:
                await ctx.send(f"{ctx.author.mention}, you do not have sufficient balance to place this sell order.")
                return

            # Check if the user has enough stocks to sell
            cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (user_id, symbol))
            user_stock = cursor.fetchone()
            if not user_stock or int(user_stock["amount"]) < quantity:
                await ctx.send(f"{ctx.author.mention}, you do not have enough stocks to place this sell order.")
                return

            # Update user stocks to remove the quantity being sold
            cursor.execute("""
                UPDATE user_stocks
                SET amount = amount - ?
                WHERE user_id = ? AND symbol = ?
            """, (quantity, user_id, symbol))

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



    @commands.command(name="open_orders", help="Show open buy/sell orders.")
    async def open_orders(self, ctx):
        user_id = ctx.author.id
        cursor = self.conn.cursor()

        # Fetch open buy orders for the user
        cursor.execute("""
            SELECT order_id, symbol, price, quantity
            FROM limit_orders
            WHERE user_id=? AND order_type='buy'
        """, (user_id,))
        buy_orders = cursor.fetchall()

        # Fetch open sell orders for the user
        cursor.execute("""
            SELECT order_id, symbol, price, quantity
            FROM limit_orders
            WHERE user_id=? AND order_type='sell'
        """, (user_id,))
        sell_orders = cursor.fetchall()

        if not buy_orders and not sell_orders:
            await ctx.send(f"{ctx.author.mention}, you do not have any open buy or sell orders.")
            return

        # Prepare and send the message
        embed = discord.Embed(
            title="Open Orders",
            description=f"Open buy/sell orders for {ctx.author.name}:",
            color=discord.Color.gold()
        )

        if buy_orders:
            buy_str = "\n".join([f"Buy {order['quantity']} shares of {order['symbol']} at {order['price']} µPPN each (ID: {order['order_id']})" for order in buy_orders])
            embed.add_field(name="Buy Orders", value=buy_str, inline=False)

        if sell_orders:
            sell_str = "\n".join([f"Sell {order['quantity']} shares of {order['symbol']} at {order['price']} µPPN each (ID: {order['order_id']})" for order in sell_orders])
            embed.add_field(name="Sell Orders", value=sell_str, inline=False)

        await ctx.send(embed=embed)


    @commands.command(name="close_order", help="Close a sell order and get back the stocks.")
    async def close_order(self, ctx, order_id: int):
        user_id = ctx.author.id

        # Check if the order exists and belongs to the user
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT order_id, symbol, price, quantity
            FROM limit_orders
            WHERE order_id=? AND user_id=? AND order_type='sell'
        """, (order_id, user_id))
        sell_order = cursor.fetchone()

        if not sell_order:
            await ctx.send(f"{ctx.author.mention}, no matching sell order found or you do not have permission to close this order.")
            return

        symbol = sell_order["symbol"]
        price = sell_order["price"]
        quantity = sell_order["quantity"]

        # Return the stocks to the user
        try:
            cursor.execute("""
                UPDATE user_stocks
                SET amount = amount + ?
                WHERE user_id = ? AND symbol = ?
            """, (quantity, user_id, symbol))

            # Remove the sell order
            cursor.execute("DELETE FROM limit_orders WHERE order_id=?", (order_id,))

            self.conn.commit()
            await ctx.send(f"{ctx.author.mention}, your sell order (ID: {order_id}) has been closed, and the stocks have been returned.")
        except sqlite3.Error as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while closing the sell order. Error: {str(e)}")



    @commands.command(name="all_open_orders", help="Show all open buy/sell orders from all users.")
    async def all_open_orders(self, ctx):
        cursor = self.conn.cursor()

        # Fetch all open buy orders
        cursor.execute("""
            SELECT user_id, symbol, price, quantity
            FROM limit_orders
            WHERE order_type='buy'
        """)
        buy_orders = cursor.fetchall()

        # Fetch all open sell orders
        cursor.execute("""
            SELECT user_id, symbol, price, quantity
            FROM limit_orders
            WHERE order_type='sell'
        """)
        sell_orders = cursor.fetchall()

        if not buy_orders and not sell_orders:
            await ctx.send("There are no open buy or sell orders.")
            return

        # Prepare and send the message
        embed = discord.Embed(
            title="All Open Orders",
            description="Open buy/sell orders from all users:",
            color=discord.Color.gold()
        )

        if buy_orders:
            buy_str = "\n".join([f"User {generate_crypto_address(order['user_id'])} wants to buy {order['quantity']} shares of {order['symbol']} at {order['price']} µPPN each" for order in buy_orders])
            embed.add_field(name="Buy Orders", value=buy_str, inline=False)

        if sell_orders:
            sell_str = "\n".join([f"User {generate_crypto_address(order['user_id'])} wants to sell {order['quantity']} shares of {order['symbol']} at {order['price']} µPPN each" for order in sell_orders])
            embed.add_field(name="Sell Orders", value=sell_str, inline=False)

        await ctx.send(embed=embed)

    @commands.command(name="check_orders", help="Check open buy/sell orders for a specified stock.")
    async def check_orders(self, ctx, symbol: str):
        cursor = self.conn.cursor()

        # Fetch open buy orders for the specified stock
        cursor.execute("""
            SELECT user_id, price, quantity
            FROM limit_orders
            WHERE order_type='buy' AND symbol=?
            ORDER BY price ASC
        """, (symbol,))
        buy_orders = cursor.fetchall()

        # Fetch open sell orders for the specified stock
        cursor.execute("""
            SELECT user_id, price, quantity
            FROM limit_orders
            WHERE order_type='sell' AND symbol=?
            ORDER BY price ASC
        """, (symbol,))
        sell_orders = cursor.fetchall()

        if not buy_orders and not sell_orders:
            await ctx.send(f"There are no open buy or sell orders for the stock symbol '{symbol}'.")
            return

        # Prepare and send the message
        embed = discord.Embed(
            title=f"Open Orders for {symbol}",
            description=f"Open buy/sell orders for {symbol} ordered from lowest to highest price:",
            color=discord.Color.gold()
        )

        if buy_orders:
            buy_str = "\n".join([f"User {generate_crypto_address(order['user_id'])} wants to buy {order['quantity']} shares at {order['price']} µPPN each" for order in buy_orders])
            embed.add_field(name="Buy Orders", value=buy_str, inline=False)

        if sell_orders:
            sell_str = "\n".join([f"User {generate_crypto_address(order['user_id'])} wants to sell {order['quantity']} shares at {order['price']} µPPN each" for order in sell_orders])
            embed.add_field(name="Sell Orders", value=sell_str, inline=False)

        await ctx.send(embed=embed)


# Buy stocks from the order book
    @commands.command(name="buy_order_book", help="Buy stocks from the order book.")
    @is_allowed_user(930513222820331590)
    async def buy_order_book(self, ctx, symbol: str, amount: int):
        buyer_id = ctx.author.id
        await ctx.message.delete()

        cursor = self.conn.cursor()


        if amount == 0:
            await ctx.send(f"{ctx.author.mention}, you cannot buy 0 amount of {stock_name}.")
            return

        # Check if the symbol is valid
        cursor.execute("SELECT user_id, price, quantity FROM limit_orders WHERE symbol=? ORDER BY price ASC, quantity ASC", (symbol,))
        orders = cursor.fetchall()

        if not orders:
            await ctx.send(f"{ctx.author.mention}, there are no open orders for the stock symbol '{symbol}'.")
            return

        total_amount_bought = 0  # Track the total amount bought to check against the requested amount

        for order in orders:
            seller_id, order_price, order_quantity = order

            # Check if the buyer has sufficient balance
            cursor.execute("SELECT balance FROM users WHERE user_id=?", (buyer_id,))
            buyer_balance = cursor.fetchone()['balance']

            total_cost = order_price * amount  # Calculate total cost for the requested amount

            if total_cost > buyer_balance:
                await ctx.send(f"{ctx.author.mention}, you do not have sufficient balance to buy these stocks.")
                return

            # Check if the daily stock limit is reached
            cursor.execute("""
                SELECT SUM(amount)
                FROM user_daily_buys
                WHERE user_id=? AND symbol=? AND DATE(timestamp)=DATE('now')
            """, (buyer_id, symbol))

            daily_bought_record = cursor.fetchone()
            daily_bought = daily_bought_record[0] if daily_bought_record and daily_bought_record[0] is not None else 0

            if daily_bought + amount > dStockLimit:
                remaining_amount = dStockLimit - daily_bought
                await ctx.send(f"{ctx.author.mention}, you have reached your daily buy limit for {symbol} stocks. "
                               f"You can buy {remaining_amount} more after 24 hours.")
                return

            # Check if there is enough quantity in the order
            if amount <= order_quantity:
                # Update buyer's balance
                new_buyer_balance = buyer_balance - total_cost
                update_user_balance(self.conn, buyer_id, new_buyer_balance)

                # Update buyer's stocks
                cursor.execute("""
                    INSERT INTO user_stocks (user_id, symbol, amount)
                    VALUES (?, ?, ?)
                    ON CONFLICT(user_id, symbol) DO UPDATE SET amount = amount + ?
                """, (buyer_id, symbol, amount, amount))

                # Update seller's balance
                cursor.execute("SELECT balance FROM users WHERE user_id=?", (seller_id,))
                seller_balance = cursor.fetchone()['balance']
                new_seller_balance = seller_balance + total_cost
                update_user_balance(self.conn, seller_id, new_seller_balance)

                # Update order book
                new_quantity = order_quantity - amount
                if new_quantity == 0:
                    cursor.execute("DELETE FROM limit_orders WHERE user_id=? AND symbol=? AND price=? AND quantity=?",
                                   (seller_id, symbol, order_price, order_quantity))
                else:
                    cursor.execute("UPDATE limit_orders SET quantity=? WHERE user_id=? AND symbol=? AND price=? AND quantity=?",
                                   (new_quantity, seller_id, symbol, order_price, order_quantity))

                total_amount_bought += amount  # Update the total amount bought

                self.conn.commit()

                formatted_buyer_balance = f"{new_buyer_balance:,.2f}"
                await log_transaction(ledger_conn, ctx, "Buy Order Book", symbol, amount, total_cost, total_cost, buyer_balance, new_buyer_balance, order_price)

                # Notify the seller in DM
                seller = self.bot.get_user(seller_id)
                if seller:
                    try:
                        await seller.send(f"Your sell order for {amount} stocks of '{symbol}' at {order_price:,.2f} each has been filled. "
                                          f"Total: {total_cost:,.2f} µPPN. New balance: {new_seller_balance:,.2f} µPPN.")
                    except discord.errors.Forbidden:
                        print(f"Unable to send a message to the seller with ID {seller_id}. They may have blocked DMs or disabled them.")

                await ctx.send(f"{ctx.author.mention}, you have bought {amount} stocks of '{symbol}' from the order book. Your new balance is: {formatted_buyer_balance} µPPN.")

                break  # Exit the loop after processing the order
            else:
                # If the requested amount is greater than the current order, process the order and continue to the next one
                total_cost = order_price * order_quantity

                # Update buyer's balance
                new_buyer_balance = buyer_balance - total_cost
                update_user_balance(self.conn, buyer_id, new_buyer_balance)

                # Update buyer's stocks
                cursor.execute("""
                    INSERT INTO user_stocks (user_id, symbol, amount)
                    VALUES (?, ?, ?)
                    ON CONFLICT(user_id, symbol) DO UPDATE SET amount = amount + ?
                """, (buyer_id, symbol, order_quantity, order_quantity))

                # Update seller's balance
                cursor.execute("SELECT balance FROM users WHERE user_id=?", (seller_id,))
                seller_balance = cursor.fetchone()['balance']
                new_seller_balance = seller_balance + total_cost
                update_user_balance(self.conn, seller_id, new_seller_balance)

                # Update order book
                cursor.execute("DELETE FROM limit_orders WHERE user_id=? AND symbol=? AND price=? AND quantity=?",
                               (seller_id, symbol, order_price, order_quantity))

                amount -= order_quantity  # Subtract the bought quantity from the requested amount

        self.conn.commit()

        # Check if the total amount bought is less than the requested amount
        if total_amount_bought < amount:
            await ctx.send(f"{ctx.author.mention}, your order could not be fulfilled completely. "
                           f"Only {total_amount_bought} stocks of '{symbol}' could be bought.")



# Stock User Tools
    @commands.command(name="my_stocks", help="Shows the user's stocks.")
    async def my_stocks(self, ctx):
        user_id = ctx.author.id
        P3Addr = generate_crypto_address(ctx.author.id)
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
                description=f"Stocks owned by {P3Addr} (Page {page + 1}/{total_pages}):",
                color=discord.Color.green()
            )

            # Calculate the range of stocks for this page
            start_idx = page * page_size
            end_idx = (page + 1) * page_size

            # Add stocks to the embed for this page
            for stock in user_stocks[start_idx:end_idx]:
                embed.add_field(name=stock['symbol'], value=f"Amount: {stock['amount']:,}", inline=True)

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
            stock_info = f"{stock[0]}: {stock[1]:,} (Price: {stock[2]:,.2f})"
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
                embed.add_field(name=f"High #{i}: {symbol}", value=f"Price: {price:,.2f} µPPN\nAvailable: {available:,}", inline=False)

            # Add fields for the top 5 lowest price stocks
            for i, (symbol, price, available) in enumerate(top_low_stocks, start=1):
                embed.add_field(name=f"Low #{i}: {symbol}", value=f"Price: {price:,.2f} µPPN\nAvailable: {available:,}", inline=False)

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

    @commands.command(name="treasure_chest", help="Opens a treasure chest and gives you shares of a stock.")
    @is_allowed_user(930513222820331590)
    async def treasure_chest(self, ctx, stock_symbol: str):
        cursor = self.conn.cursor()
        cursor.execute("SELECT symbol, available FROM stocks WHERE symbol=?", (stock_symbol,))
        stock = cursor.fetchone()

        if stock is None:
            await ctx.send(f"No stock with symbol {stock_symbol} found.")
            return

        embed = discord.Embed(
            title="Treasure Chest",
            description=f"React with 💰 to claim your potential reward of {treasureMin} to {treasureMax} shares of {stock_symbol}! 🎉",
            color=0xFFD700,
        )
        message = await ctx.send(embed=embed)

        await message.add_reaction("💰")

        def reaction_check(reaction, user):
            return (
                str(reaction.emoji) == "💰"
                and user != self.bot.user
                and not user.bot
                and user.id not in self.claimed_users
            )

        try:
            while True:
                reaction, user = await self.bot.wait_for(
                    "reaction_add", timeout=1800, check=reaction_check
                )

                reward_amount = random.randint(treasureMin, treasureMax)

                reward_message = f"{user.mention}, you received {reward_amount} shares of {stock_symbol}! 🎉"
                await ctx.send(reward_message)

                cursor.execute(
                    "SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?",
                    (user.id, stock_symbol),
                )
                user_stock = cursor.fetchone()

                if user_stock is None:
                    cursor.execute(
                        "INSERT INTO user_stocks(user_id, symbol, amount) VALUES(?, ?, ?)",
                        (user.id, stock_symbol, reward_amount),
                    )
                else:
                    new_amount = user_stock["amount"] + reward_amount
                    cursor.execute(
                        "UPDATE user_stocks SET amount=? WHERE user_id=? AND symbol=?",
                        (new_amount, user.id, stock_symbol),
                    )

                new_available = stock["available"] - reward_amount
                cursor.execute(
                    "UPDATE stocks SET available=? WHERE symbol=?", (new_available, stock_symbol)
                )

                await log_stock_transfer(ledger_conn, ctx, self.bot.user, user, stock_symbol, reward_amount)

                self.conn.commit()

                # Add the user to the set of claimed users
                self.claimed_users.add(user.id)

                #Boost P3:Treasure_Chest
                await self.treasureBooster(ctx)

        except asyncio.TimeoutError:
            await ctx.send("The treasure chest has been closed. Try again later!")



    @commands.command(name='wipe_tables', help='Wipe all tables in the P3addr.db database.')
    @is_allowed_user(930513222820331590)
    async def wipe_tables(self, ctx):
        # Connect to the P3addr.db database
        conn = sqlite3.connect("P3addr.db")

        # Create a cursor object to execute SQL commands
        cursor = conn.cursor()

        # Execute the SQL command to drop the user_addresses table
        cursor.execute('DROP TABLE IF EXISTS user_addresses')

        # Recreate the user_addresses table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_addresses (
                user_id TEXT PRIMARY KEY,
                p3_address TEXT NOT NULL,
                vanity_address TEXT NOT NULL DEFAULT ''
            )
        ''')

        # Commit the changes and close the connection
        await ctx.send("Tables wiped and recreated successfully.")
        conn.commit()
        conn.close()

    @commands.command(name="my_addr", help="")
    async def my_addr(self, ctx):
        conn = sqlite3.connect("P3addr.db")
        # Get the Discord User ID of the invoking user
        discord_user_id = ctx.author.id

        # Generate the crypto address
        crypto_address = generate_crypto_address(discord_user_id)

        # Get the vanity address (if available)
        vanity_address = get_vanity_address(conn, discord_user_id)

        # Create an embed to display the information
        embed = discord.Embed(title="Your P3 Address", color=0x00ff00)
        embed.add_field(name="Personal P3 Address", value=crypto_address, inline=False)

        # Include vanity address if available
        if vanity_address:
            embed.add_field(name="Vanity Address", value=vanity_address, inline=False)

        # Send the embed as a reply
        await ctx.send(embed=embed)

# Command to store a P3 address
    @commands.command(name='store_addr')
    async def store_addr(self, ctx, target=None):
        # Connect to the P3addr.db database
        conn = sqlite3.connect("P3addr.db")

        # Get the user's ID
        user_id = str(ctx.author.id)

        # Check if the user has already stored an address
        if has_stored_address(conn, user_id):
            await ctx.send("You have already stored a P3 address.")
            conn.close()
            return

        # If no target is provided, store the address for the invoking user
        if not target:
            p3_address = generate_crypto_address(user_id)
        else:
            # If a target is provided, try to find the mentioned user
            try:
                # Remove potential mention symbols
                target = target.strip("<@!>")
                # Fetch the mentioned user's ID
                target_user_id = int(target)
                # Generate a P3 address for the mentioned user
                p3_address = generate_crypto_address(target_user_id)
            except ValueError:
                await ctx.send("Invalid target. Please mention a user or leave it blank to store your own P3 address.")
                conn.close()
                return

        # Store the P3 address in the database
        cursor = conn.cursor()
        cursor.execute("INSERT INTO user_addresses (user_id, p3_address) VALUES (?, ?)", (user_id, p3_address))
        conn.commit()

        await ctx.send(f"P3 address stored successfully: {p3_address}")

        # Close the database connection
        conn.close()


    @commands.command(name='store_user_addr', help='Store the P3 address of a mentioned user.')
    async def store_user_addr(self, ctx, target: discord.Member):
        # Connect to the P3addr.db database
        conn = sqlite3.connect("P3addr.db")

        # Get the user's ID
        user_id = str(target.id)

        # Check if the user has already stored an address
        if has_stored_address(conn, user_id):
            await ctx.send("You have already stored a P3 address.")
            conn.close()
            return

        # Generate a P3 address for the mentioned user
        p3_address = generate_crypto_address(target.id)

        # Store the P3 address in the database
        cursor = conn.cursor()
        cursor.execute("INSERT INTO user_addresses (user_id, p3_address) VALUES (?, ?)", (user_id, p3_address))
        conn.commit()

        await ctx.send(f"P3 address for {target.mention} stored successfully: {p3_address}")

        # Close the database connection
        conn.close()


    @commands.command(name='store_all_users', help='Store P3 addresses for all non-bot users in the server.')
    async def store_all_users(self, ctx):
        # Connect to the P3addr.db database
        conn = sqlite3.connect("P3addr.db")

        # Get all non-bot members in the server
        non_bot_members = [member for member in ctx.guild.members if not member.bot]

        # Iterate over non-bot members and store their P3 addresses
        for member in non_bot_members:
            user_id = str(member.id)

            # Check if the user already has a stored address
            if has_stored_address(conn, user_id):
                await ctx.send(f"{member.mention} already has a P3 address stored.")
                continue

            # Generate a P3 address for the member
            p3_address = generate_crypto_address(member.id)

            # Store the P3 address in the database
            cursor = conn.cursor()
            cursor.execute("INSERT INTO user_addresses (user_id, p3_address) VALUES (?, ?)", (user_id, p3_address))
            conn.commit()

            await ctx.send(f"P3 address for {member.mention} stored successfully: {p3_address}")

        # Close the database connection
        conn.close()




    @commands.command(name='assign_vanity_addr', help='Assign a vanity P3 address to a user.')
    @is_allowed_user(930513222820331590)
    async def assign_vanity_addr(self, ctx, target, vanity_address):
        # Connect to the P3addr.db database
        conn = sqlite3.connect("P3addr.db")

        # Get the user's ID based on the input (mention, user ID, or P3 address)
        user_id = get_user_id_from_input(conn, target)

        if not user_id:
            await ctx.send("Invalid user. Please provide a valid mention, user ID, or P3 address.")
            conn.close()
            return

        # Check if the vanity address is unique
        if not is_vanity_address_unique(conn, vanity_address):
            await ctx.send("Vanity address is not unique. Please choose a different one.")
            conn.close()
            return

        # Update the user's vanity address in the database
        cursor = conn.cursor()
        cursor.execute("UPDATE user_addresses SET vanity_address=? WHERE user_id=?", (vanity_address, user_id))
        conn.commit()

        await ctx.send(f"Vanity address for user {target} assigned successfully: {vanity_address}")

        # Close the database connection
        conn.close()

# Command to look up user information
    @commands.command(name='whois')
    async def whois(self, ctx, target):
        # Connect to the P3addr.db database
        conn = sqlite3.connect("P3addr.db")

        # Check if the target is a mention (Discord username)
        if ctx.message.mentions:
            user_id = str(ctx.message.mentions[0].id)
            p3_address = get_p3_address(conn, user_id)
            if p3_address:
                await ctx.send(f"{ctx.message.mentions[0].mention}'s P3 address: {p3_address}")
            else:
                await ctx.send("User has not stored a P3 address.")

        # Check if the target is a P3 address or vanity address
        elif target.startswith("P3:"):
            p3_address = target
            user_id = get_user_id(conn, p3_address)
            if user_id:
                await ctx.send(f"The owner of {p3_address} is <@{user_id}>.")
            else:
                user_id = get_user_id_by_vanity(conn, target)
                if user_id:
                    p3_address = get_p3_address(conn, user_id)
                    await ctx.send(f"The owner of vanity address {target} is <@{user_id}>. P3 address: {p3_address}")
                else:
                    await ctx.send("Invalid or unknown user, P3 address, or vanity address.")


        else:
            await ctx.send("Invalid or unknown user, P3 address, or vanity address.")

        # Close the database connection
        conn.close()

    @commands.command(name="show_users", help="Show the number of users with stored P3 addresses.")
    async def show_users(self, ctx):
        P3conn = sqlite3.connect("P3addr.db")

        cursor = P3conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM user_addresses")

        count = cursor.fetchone()[0]

        await ctx.send(f"There are {count} users with stored P3 addresses.")

        P3conn.close()

##
    @commands.command(name="give_stock", help="Give a user an amount of a stock. Deducts it from the total supply.")
    @is_allowed_user(930513222820331590)  # The user must have the 'admin' role to use this command.
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
    async def send_stock(self, ctx, target, symbol: str, amount: int):
        cursor = self.conn.cursor()
        P3addrConn = sqlite3.connect("P3addr.db")


        await ctx.message.delete()

        if target.startswith("P3:"):
            p3_address = target
            user_id = get_user_id(P3addrConn, p3_address)
            if not user_id:
                await ctx.send("Invalid or unknown P3 address.")
                return
        else:
            await ctx.send("Please provide a valid P3 address.")
            return

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
        cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (user_id, symbol))
        recipient_stock = cursor.fetchone()
        if recipient_stock is None:
            cursor.execute("INSERT INTO user_stocks(user_id, symbol, amount) VALUES(?, ?, ?)", (user_id, symbol, amount))
        else:
            new_amount = recipient_stock['amount'] + amount
            cursor.execute("UPDATE user_stocks SET amount=? WHERE user_id=? AND symbol=?", (new_amount, user_id, symbol))

        self.conn.commit()

        # Log the stock transfer
        await log_stock_transfer(ledger_conn, ctx, ctx.author, user_id, symbol, amount)

        await ctx.send(f"Sent {amount} of {symbol} to {target}.")


    @commands.command(name="add_stock", help="Add a new stock. Provide the stock symbol, name, price, total supply, and available amount.")
    @is_allowed_user(930513222820331590, PBot)
    async def add_stock(self, ctx, symbol: str, name: str, price: int, total_supply: int, available: int):
        cursor = self.conn.cursor()
        cursor.execute("INSERT INTO stocks (symbol, name, price, total_supply, available) VALUES (?, ?, ?, ?, ?)",
                       (symbol, name, price, total_supply, available))
        self.conn.commit()
        await ctx.send(f"Added new stock: {symbol} ({name}), price: {price}, total supply: {total_supply}, available: {available}")



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
            else:
                await ctx.send(f"{symbol} is not a valid stock symbol.")
                return

            # Get the top holders of the stock
            cursor.execute("""
                SELECT user_id, amount
                FROM user_stocks
                WHERE symbol = ?
                ORDER BY amount DESC
                LIMIT 10
            """, (symbol,))
            top_holders = cursor.fetchall()

            # Create an embed to display the stock information
            embed = discord.Embed(
                title=f"Stock Information for {symbol}",
                color=discord.Color.blue()
            )
            embed.add_field(name="Available Supply", value=f"{available} shares", inline=False)

            if top_holders:
                top_holders_str = "\n".join([f"{generate_crypto_address(user_id)} - {amount} shares" for user_id, amount in top_holders])
                embed.add_field(name="Top Holders", value=top_holders_str, inline=False)
            else:
                embed.add_field(name="Top Holders", value="No one currently holds shares.", inline=False)

            await ctx.send(embed=embed)

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while fetching stock information: {e}")



# Stock Engine

    @commands.command(name="ip")
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


    @commands.command(name="dp")
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


    @commands.command(name="buy_stock_for_bot")
    @is_allowed_user(930513222820331590, PBot)
    async def buy_stock_for_bot(self, ctx, stock_name, amount):
        cursor = self.conn.cursor()

        # Get the stock information
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
        stock_info = cursor.fetchone()

        if stock_info is None:
            await ctx.send(f"This stock '{stock_name}' does not exist.")
            return

        price = Decimal(stock_info[2])
        total_cost = price * Decimal(amount)

        # Update bot's stock balance
        cursor.execute("""
            INSERT INTO user_stocks (user_id, symbol, amount)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, symbol) DO UPDATE SET amount = amount + ?
        """, (PBot, stock_name, amount, amount))

        # Update available stock
        cursor.execute("""
            UPDATE stocks
            SET available = available - ?
            WHERE symbol = ?
        """, (amount, stock_name))


        # Update bot's balance
        current_balance = get_user_balance(self.conn, PBot)
        new_balance = current_balance + total_cost
        update_user_balance(self.conn, PBot, new_balance)
        decay_other_stocks(self.conn, stock_name)
        await self.increase_price_non_verbose(ctx, stock_name, float(amount))
        await log_transaction(ledger_conn, ctx, "Buy Stock", stock_name, amount, total_cost, total_cost, 0, 0, price)

        # Commit the transaction
        self.conn.commit()

    @commands.command(name="sell_stock_for_bot")
    @is_allowed_user(930513222820331590, PBot)
    async def sell_stock_for_bot(self, ctx, stock_name, amount):
        cursor = self.conn.cursor()

        # Get the stock information
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
        stock_info = cursor.fetchone()

        if stock_info is None:
            await ctx.send(f"This stock '{stock_name}' does not exist.")
            return

        price = Decimal(stock_info[2])
        total_earnings = price * Decimal(amount)

        # Update bot's stock balance
        cursor.execute("""
            UPDATE user_stocks
            SET amount = amount - ?
            WHERE user_id = ? AND symbol = ?
        """, (amount, PBot, stock_name))

        # Update available stock
        cursor.execute("""
            UPDATE stocks
            SET available = available + ?
            WHERE symbol = ?
        """, (amount, stock_name))

        # Update bot's balance
        current_balance = get_user_balance(self.conn, PBot)
        new_balance = current_balance + total_earnings
        update_user_balance(self.conn, PBot, new_balance)
        decay_other_stocks(self.conn, stock_name)
        await self.decrease_price_non_verbose(ctx, stock_name, float(amount))
        await log_transaction(ledger_conn, ctx, "Sell Stock", stock_name, amount, total_earnings, total_earnings, 0, 0, price)

        # Commit the transaction
        self.conn.commit()


    @commands.command(name="reward_share_holders", help="Reward P3:Stable shareholders with 7% more shares.")
    @is_allowed_user(930513222820331590)
    async def reward_share_holders(self, ctx, stock_symbol: str):
        cursor = self.conn.cursor()


        if stock_symbol.lower() == "p3:stable":
            stable_symbol = "P3:Stable"
            stakingYield = 0.07
        elif stock_symbol.lower() == "p3:bank":
            stable_symbol = "P3:BANK"
            stakingYield = 0.12
        elif stock_symbol.lower() == "p3:lqdy":
            stable_symbol = "P3:LQDY"
            stakingYield = 0.0475
        else:
            ctx.send(f"{stock_symbol} is not a Yield Reward Stock")
            return

        # Fetch all users who hold P3:Stable
        cursor.execute("SELECT user_id, amount FROM user_stocks WHERE symbol=?", (stable_symbol,))
        stable_holders = cursor.fetchall()

        if not stable_holders:
            await ctx.send("No users hold P3:Stable.")
            return

        total_rewarded_shares = 0

        # Reward each shareholder
        for holder in stable_holders:
            user_id = holder['user_id']
            if user_id == 1092870544988319905:
                print(f"Skipping {user_id}")
                continue
            amount_held = holder['amount']
            if amount_held == 0.0:
                continue
            print(f"UserID: {user_id} holds {amount_held} of {stable_symbol}")

            # Calculate the reward (7% of the held amount)
            reward_shares = int(amount_held * stakingYield)

            # Update the user's stock holdings with the rewarded shares
            new_amount_held = amount_held + reward_shares
            cursor.execute("UPDATE user_stocks SET amount=? WHERE user_id=? AND symbol=?", (new_amount_held, user_id, stable_symbol))

            total_rewarded_shares += reward_shares
            await log_stock_transfer(ledger_conn, ctx, "stakingYield", user_id, stable_symbol, reward_shares)


        self.conn.commit()

        if stock_symbol.lower() != "p3:lqdy":
            await ctx.send(f"Reward distribution completed. Total {stock_symbol} shares rewarded: {total_rewarded_shares:,.2f} at {stakingYield * 100}%.")
        else:
            print(f"{stock_symbol} utility deployed")



    @commands.command(name="stableManager")
    @is_allowed_user(930513222820331590, PBot)
    async def stableManager(self, ctx, type: str, amount: int):
        cursor = self.conn.cursor()

        reserveStocks = ["P3:Gold-Reserve", "BlueChipOG"]

        shareLimit = 1000000000



        if amount >= shareLimit:
            shares_amount = shareLimit
        else:
            shares_amount = amount

        if type.lower() == "buy":
            for stock_name in reserveStocks:
                await self.sell_stock_for_bot(ctx, stock_name, shares_amount)
            # Randomly fluctuate the price of P3:Stable
            new_price = random.uniform(850, 2500)
            cursor.execute("""
                UPDATE stocks
                SET price = ?
                WHERE symbol = "P3:Stable"
            """, (new_price,))
            self.conn.commit()

            await ctx.send(f"P3:Stable has been bought, and its price has fluctuated to {new_price:,.2f}.")

        elif type.lower() == "sell":
            for stock_name in reserveStocks:
                await self.buy_stock_for_bot(ctx, stock_name, shares_amount * 2)

            # Randomly fluctuate the price of P3:Stable
            new_price = random.uniform(850, 2500)
            cursor.execute("""
                UPDATE stocks
                SET price = ?
                WHERE symbol = "P3:Stable"
            """, (new_price,))
            self.conn.commit()

            await ctx.send(f"P3:Stable has been sold, and its price has fluctuated to {new_price:,.2f}.")

        else:
            await ctx.send("Invalid operation. Use 'buy' or 'sell'.")



    @commands.command(name="blueChipBooster")
    @is_allowed_user(930513222820331590, PBot)
    async def blueChipBooster(self, ctx, type: str):
        if type == "BUY":
            boosterAmount = 75000000
        elif type == "SELL":
            boosterAmount = 150000000
        else:
            return
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", ("BlueChipOG",))
        stock = cursor.fetchone()
        if stock is None:
            await ctx.send(f"This stock does not exist.")
            return

        price_increase = random.uniform(buyPressureMin * boosterAmount, min(buyPressureMax * boosterAmount, 1000000 - float(stock[2])))
        current_price = float(stock[2])
        new_price = min(current_price + price_increase, stockMax)

        # Update the stock price
        cursor.execute("""
            UPDATE stocks
            SET price = ?
            WHERE symbol = ?
        """, (new_price, 'BlueChipOG'))

        self.conn.commit()
        cost = boosterAmount * current_price

        print(f'BlueChipBooster: {boosterAmount} shares')
        await self.buy_stock_for_bot(ctx, "BlueChipOG", boosterAmount)

    @commands.command(name="P3LQDYBooster")
    @is_allowed_user(930513222820331590, PBot)
    async def P3LQDYBooster(self, ctx):
        stock_name = "P3:LQDY"
        etf_id = "6"
        try:
            booster_amount = 45000000
            cursor = conn.cursor()

            # Get the current stock information
            cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_symbol,))
            stock = cursor.fetchone()

            if stock is None:
                return f"This stock ({stock_symbol}) does not exist."

            # Get the ETF value
            etf_value = await get_etf_value(conn, etf_id)

            # Calculate the new stock price based on inverse correlation
            current_price = float(stock[2])
            inverse_price = current_price + booster_amount * (1 - etf_value / (current_price * booster_amount))

            # Update the stock price
            cursor.execute("""
                UPDATE stocks
                SET price = ?
                WHERE symbol = ?
            """, (inverse_price, stock_symbol))

            self.conn.commit()

            print(f"{booster_amount} shares for {stock_symbol} updated based on inverse correlation with ETF.")

        except sqlite3.Error as e:
            return f"An error occurred: {str(e)}"

        except Exception as e:
            return f"An unexpected error occurred: {str(e)}"



    @commands.command(name="goldReserveBooster")
    @is_allowed_user(930513222820331590, PBot)
    async def goldReserveBooster(self, ctx, boosterAmount: float):
        if boosterAmount >= 500000000:
            boosterAmount = (boosterAmount * 0.5 * 0.25 * 0.26 * 0.5)
        else:
            boosterAmount = 25000000
        stock_name = "P3:Gold-Reserve"
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
        stock = cursor.fetchone()
        if stock is None:
            await ctx.send(f"This stock does not exist.")
            return

        current_price = float(stock[2])


        cost = boosterAmount * current_price
        await self.buy_stock_for_bot(ctx, stock_name, boosterAmount)
        self.conn.commit()


    @commands.command(name="treasureBooster")
    @is_allowed_user(930513222820331590, PBot)
    async def treasureBooster(self, ctx):
        boosterAmount = 75000000
        stock_name = "P3:Treasure_Chest"
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
        stock = cursor.fetchone()
        if stock is None:
            await ctx.send(f"This stock does not exist.")
            return

        current_price = float(stock[2])


        cost = boosterAmount * current_price
        self.conn.commit()
        await self.buy_stock_for_bot(ctx, stock_name, boosterAmount)


    @commands.command(name="whaleBooster")
    @is_allowed_user(930513222820331590, PBot)
    async def whaleBooster(self, ctx):
        boosterAmount = 75000000
        stock_name = "🐳"
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
        stock = cursor.fetchone()
        if stock is None:
            await ctx.send(f"This stock does not exist.")
            return

        current_price = float(stock[2])

        cost = boosterAmount * current_price
        self.conn.commit()
        await self.buy_stock_for_bot(ctx, stock_name, boosterAmount)

    @commands.command(name="casinoTool")
    @is_allowed_user(930513222820331590, PBot)
    async def casinoTool(self, ctx, resultWin: bool):
        stock_name = "P3:Casino"
        cursor = self.conn.cursor()

        # Retrieve current supply amounts
        cursor.execute("SELECT total_supply, available FROM stocks WHERE symbol=?", (stock_name,))
        supply_info = cursor.fetchone()
        if supply_info is None:
            await ctx.send(f"This stock does not exist.")
            return

        total_supply, current_supply = supply_info

        booster_amount = None
        supply_boost = None

        if resultWin:
            booster_amount = 50000000
            supply_boost = 15000000
        else:
            booster_amount = 15000000
            supply_boost = 50000000

        # Apply booster amount to total and available supply
        total_supply += supply_boost * 2
        current_supply += supply_boost * 2

        # Update the stock supply
        await self.set_stock_supply(ctx, stock_name, total_supply, current_supply)

        # Update the stock price
        stock = cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,)).fetchone()
        if stock:
            current_price = float(stock[2])


            # Send an embed to the ledger channel
            guild_id = 1161678765894664323
            ledger_channel_id = 1161680453841993839
            ledger_channel = ctx.guild.get_channel(ledger_channel_id)

            if ledger_channel:
                embed = discord.Embed(
                    title=f"Shares Unlocked for {stock_name}",
                    description=f"{supply_boost} shares have been unlocked for {stock_name}.",
                    color=discord.Color.green()
                )
                embed.add_field(name="Current Price", value=f"{current_price:,.2f} µPPN", inline=False)
                embed.add_field(name="Total Supply", value=f"{total_supply:,.2f} shares", inline=True)
                embed.add_field(name="Available Supply", value=f"{current_supply:,.2f} shares", inline=True)

                await ledger_channel.send(embed=embed)

            cost = booster_amount * current_price
            await ctx.send(f"{supply_boost:,.2f} Shares unlocked for {stock_name}")
            await self.buy_stock_for_bot(ctx, stock_name, booster_amount / 2)



    @commands.command(name="sludgeBoost")
    @is_allowed_user(930513222820331590, PBot)
    async def sludgeBoost(self, ctx, amount: int):
        boosterAmount = 75000000
        if amount < boosterAmount:
            print(f"Debug:  quantity {amount:,} less than required {boosterAmount:,}")
            return
        stock_name = ["SS:HEAT", "SS:OG", "SS:Joseph", "SS:FIRE", "SSOG:SLUDGE"]
        for ss_stock in stock_name:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM stocks WHERE symbol=?", (ss_stock,))
            stock = cursor.fetchone()
            if stock is None:
                await ctx.send(f"This stock does not exist.")
                return

            current_price = float(stock[2])
            cost = boosterAmount * current_price
            self.conn.commit()
            await self.buy_stock_for_bot(ctx, ss_stock, boosterAmount)


    @commands.command(name="increase_price")
    @is_allowed_user(930513222820331590, PBot)
    async def increase_price(self, ctx, stock_name: str, amount: float):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
        stock = cursor.fetchone()
        if stock is None:
            await ctx.send(f"This stock does not exist.")
            return

        price_increase = random.uniform(buyPressureMin * amount, min(buyPressureMax * amount, 1000000 - float(stock[2])))
        current_price = float(stock[2])
        new_price = min(current_price + price_increase, stockMax)

        # Update the stock price
        cursor.execute("""
            UPDATE stocks
            SET price = ?
            WHERE symbol = ?
        """, (new_price, stock_name))

        self.conn.commit()

        await ctx.send(f"The price of {stock_name} has increased to {new_price:,.2f}.")

    @commands.command(name="increase_price_non_verbose")
    @is_allowed_user(930513222820331590, PBot)
    async def increase_price_non_verbose(self, ctx, stock_name: str, amount: float):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
        stock = cursor.fetchone()
        if stock is None:
            await ctx.send(f"This stock does not exist.")
            return

        # Generate a random float between 0.000001 * amount and 0.000035 * amount for price increase
        price_increase = random.uniform(buyPressureMin * amount, min(buyPressureMax * amount, 1000000 - float(stock[2])))
        current_price = float(stock[2])
        new_price = min(current_price + price_increase, stockMax)  # Ensure the price doesn't go above 150,000

        # Update the stock price
        cursor.execute("""
            UPDATE stocks
            SET price = ?
            WHERE symbol = ?
        """, (new_price, stock_name))

        self.conn.commit()



    @commands.command(name="decrease_price")
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
        current_price = float(stock[2])
        new_price = max(current_price - price_decrease, stockMin)  # Ensure the price doesn't go below 0.00001

        # Update the stock price
        cursor.execute("""
            UPDATE stocks
            SET price = ?
            WHERE symbol = ?
        """, (float(new_price), stock_name))

        self.conn.commit()

        await ctx.send(f"The price of {stock_name} has decreased to {new_price:,.2f}.")



    @commands.command(name="decrease_price_non_verbose")
    @is_allowed_user(930513222820331590, PBot)
    async def decrease_price_non_verbose(self, ctx, stock_name: str, amount: float):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
        stock = cursor.fetchone()
        if stock is None:
            await ctx.send(f"This stock does not exist.")
            return

        # Generate a random float between 0.000011 * amount and 0.00005 * amount for price decrease
        price_decrease = random.uniform(sellPressureMin * amount, min(sellPressureMax * amount, float(stock[2])))
        current_price = float(stock[2])
        new_price = max(current_price - price_decrease, stockMin)  # Ensure the price doesn't go below 0.00001

        # Update the stock price
        cursor.execute("""
            UPDATE stocks
            SET price = ?
            WHERE symbol = ?
        """, (float(new_price), stock_name))

        self.conn.commit()

        print(f"The price of {stock_name} has decreased to {new_price:,.2f}.")


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

            embed.add_field(name=f"ETF ID: {etf_id}", value=f"Name: {etf_name}\nValue: {etf_value:,.2f} µPPN", inline=False)

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

            embed.add_field(name=f"ETF ID: {etf_id}", value=f"Name: {etf_name}\nQuantity: {quantity}\nValue: {(etf_value or 0) * quantity:,.2f} µPPN", inline=False)

        embed.set_footer(text=f"Total Value: {total_value:,.2f} µPPN")
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
            embed.add_field(name="Stock", value="Price (µPPN) - Available Supply", inline=False)
            for stock in stock_chunk:
                symbol = stock[0]
                price = stock[1]
                available = stock[2]
                embed.add_field(name=symbol, value=f"${price:,.2f} - {available} available", inline=False)
            if len(stock_chunks) > 1:
                embed.set_footer(text=f"Page {i}/{len(stock_chunks)}")
            embeds.append(embed)

        # Calculate the total ETF value
        total_etf_value = etf_price

        # Add a field to the last embed with the total ETF value
        embeds[-1].add_field(name="Total ETF Value", value=f"${total_etf_value:,.2f}", inline=False)

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
            description=f"{percentage_change:,.2f}%",
            color=discord.Color.green()
        )

        # Send the embed message to the channel
        await ctx.send(embed=embed)

    @commands.command(name="market_polling_6", help="Update the name of the ETF 6 voice channel with its value.")
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
            voice_channel_id = 1161706930981589094  # Replace with the actual ID
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
                        value=f"${old_price:,.2f}",
                        inline=True
                    )
                    embed.add_field(
                        name="New Price",
                        value=f"${etf_6_value:,.2f}",
                        inline=True
                    )
                    embed.add_field(
                        name="Percentage Change",
                        value=f"{percentage_change:,.2f}%",
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
            current_time_minutes = time.time() / 60
            for interval, old_value in StockBot.etf_values.items():
                if old_value is not None:
                    elapsed_time = current_time_minutes - int(interval.split()[0])
                    if elapsed_time > 0:
                        percentage_change = ((etf_6_value - old_value) / old_value) * 100
                        await self.send_percentage_change_embed(ctx, interval, percentage_change)
                        print(f"Elapsed Time ({interval}): {elapsed_time} minutes")
                        print(f"Old Value ({interval}): {old_value}")


            # Wait for 120 seconds before checking again
            await asyncio.sleep(300)



# Buy/Sell ETFs
    @commands.command(name="buy_etf", help="Buy an ETF. Provide the ETF ID and quantity.")
    async def buy_etf(self, ctx, etf_id: int, quantity: int):
        user_id = ctx.author.id
        cursor = self.conn.cursor()

        await ctx.message.delete()

        # Check if user already holds the maximum allowed quantity of the ETF
        cursor.execute("SELECT COALESCE(SUM(quantity), 0) FROM user_etfs WHERE user_id=? AND etf_id=?", (user_id, etf_id))
        current_holding = cursor.fetchone()[0]



        if int(current_holding) + int(quantity) >= int(dETFLimit):
            print(f'Max ETF for ETF: {etf_id}, Userid: {user_id}, P3 Address: {generate_crypto_address(user_id)}')
            await ctx.send("Reached Max ETF Limit")
            return

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
            await ctx.send(f"{ctx.author.mention}, you do not have enough µPPN to buy this etf. You need {missing_amount:,.2f} more µPPN, including tax, to complete this purchase.")
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
        await self.blueChipBooster(ctx, "BUY")
        self.conn.commit()
        print(f'ETF BUY for ETF: {etf_id}, Userid: {user_id}, P3 Address: {generate_crypto_address(user_id)}')
        await ctx.send(f"You have successfully bought {quantity} units of ETF {etf_id}. Your new balance is: {new_balance:,.2f} µPPN.")
        if etf_id == 10:
            await self.whaleBooster(ctx)
        if etf_id == 3:
            await self.sludgeBoost(ctx, quantity)
            print("SludgeBooster")





    @commands.command(name="sell_etf", help="Sell an ETF. Provide the ETF ID and quantity.")
    async def sell_etf(self, ctx, etf_id: int, quantity: int):
        user_id = ctx.author.id
        member = ctx.guild.get_member(user_id)
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
        if has_role(member, bronze_pass) and tax_rate >= 0.15:
            tax_percentage -= role_discount
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
        await self.blueChipBooster(ctx, "SELL")
        self.conn.commit()
        if etf_id == 10:
            await self.whaleBooster(ctx)

        await ctx.send(f"You have successfully sold {quantity} units of ETF {etf_id}. Your new balance is: {new_balance:,.2f} µPPN.")

    @commands.command(name="buy_all_etf", help="Buy a specified quantity of each ETF up to a limit of 500 billion shares.")
    async def buy_all_etf(self, ctx, quantity: int):
        user_id = ctx.author.id
        max_share_limit = dETFLimit # 500 billion shares

        cursor = self.conn.cursor()

        # Fetch the list of ETFs
        cursor.execute("SELECT etf_id FROM etfs")
        etf_ids = [row[0] for row in cursor.fetchall()]

        total_shares = 0

        for etf_id in etf_ids:
            # Check if the user already holds any units of the specified ETF
            cursor.execute("SELECT quantity FROM user_etfs WHERE user_id=? AND etf_id=?", (user_id, etf_id))
            current_holding = cursor.fetchone()
            if etf_id == 10:
                await self.whaleBooster(ctx)

            if current_holding is None or current_holding[0] <= 0:
                current_quantity = 0
            else:
                current_quantity = current_holding[0]

            # Calculate the remaining shares the user can buy
            remaining_shares = max_share_limit - current_quantity

            if remaining_shares <= 0:
                await ctx.send(f"You have reached the maximum limit of {max_share_limit} shares for ETF {etf_id}. Purchase aborted.")
                return

            # Check if the user is trying to buy more shares than the remaining limit
            if quantity > remaining_shares:
                await ctx.send(f"You can only buy up to {remaining_shares} more shares of ETF {etf_id}. Purchase aborted.")
                return

            # Fetch the ETF's value and calculate the total cost
            cursor.execute("SELECT SUM(stocks.price * etf_stocks.quantity) FROM etf_stocks JOIN stocks ON etf_stocks.symbol = stocks.symbol WHERE etf_stocks.etf_id=?", (etf_id,))
            etf_value = cursor.fetchone()[0]

            if etf_value is None:
                await ctx.send(f"Invalid ETF ID {etf_id}. Skipping this ETF.")
            else:
                cost = Decimal(etf_value) * Decimal(quantity)
                total_shares += quantity

                # Check if the user has sufficient balance
                current_balance = get_user_balance(self.conn, user_id)
                if cost > current_balance:
                    await ctx.send(f"You do not have sufficient balance to buy {quantity} shares of ETF {etf_id}.")
                    return

                # Update user's balance
                new_balance = current_balance - cost
                try:
                    update_user_balance(self.conn, user_id, new_balance)
                except ValueError as e:
                    await ctx.send(f"An error occurred while updating the user balance. Error: {str(e)}")
                    return

                # Add user's ETF holdings for the specified ETF
                try:
                    cursor.execute("INSERT INTO user_etfs (user_id, etf_id, quantity) VALUES (?, ?, ?) ON CONFLICT(user_id, etf_id) DO UPDATE SET quantity = quantity + ?", (user_id, etf_id, quantity, quantity))
                except sqlite3.Error as e:
                    await ctx.send(f"An error occurred while updating user ETF holdings. Error: {str(e)}")
                    return

                decay_other_stocks(self.conn, "P3:BANK")
                await self.blueChipBooster(ctx, "BUY")
                await log_transaction(ledger_conn, ctx, "Buy ALL ETF", etf_id, quantity, cost, cost, current_balance, new_balance, etf_value)

        self.conn.commit()
        await ctx.send(f"You have successfully bought {total_shares} shares of each ETF. Your new balance is: {new_balance:,.2f} µPPN.")



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
            await self.blueChipBooster(ctx, "SELL")
            if etf_id == 10:
                await self.whaleBooster(ctx)

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
            await ctx.send(f"You have successfully sold all units of ETFs {sold_etf_list}. Your new balance is: {new_balance:,.2f} µPPN.")


    @commands.command(name="reset_stock_limit", help="Reset a user's daily stock buy limit based on P3 address.")
    @is_allowed_user(930513222820331590, PBot)
    async def reset_stock_limit(self, ctx, target_address: str, stock_symbol: str):
        # Connect to the P3addr.db database
        p3addr_conn = sqlite3.connect("P3addr.db")

        # Get the user_id associated with the target address
        user_id = get_user_id(p3addr_conn, target_address)

        if not user_id:
            await ctx.send("Invalid or unknown P3 address.")
            return

        # Connect to the currency_system database
        currency_conn = sqlite3.connect("currency_system.db")
        cursor = currency_conn.cursor()

        try:
            # Check if the user has a daily stock buy record
            cursor.execute("""
                SELECT *
                FROM user_daily_buys
                WHERE user_id=? AND symbol=? AND timestamp >= date('now', '-1 day')
            """, (user_id, stock_symbol))
            stock_limit_record = cursor.fetchone()

            if stock_limit_record:
                # Reset the daily stock buy record
                cursor.execute("""
                    DELETE FROM user_daily_buys
                    WHERE user_id=? AND symbol=?
                """, (user_id, stock_symbol))
                currency_conn.commit()

                await ctx.send(f"Successfully reset daily stock buy limit for the user with P3 address {target_address} and stock {stock_symbol}.")
            else:
                await ctx.send(f"This user did not reach the daily stock buy limit for {stock_symbol} yet.")
        except sqlite3.Error as e:
            await ctx.send(f"An error occurred: {str(e)}")
        finally:
            # Close the database connections
            p3addr_conn.close()
            currency_conn.close()

    @commands.command(name="reset_stock_limit_admin", help="Reset a user's daily stock buy limit based on P3 address.")
    @is_allowed_user(930513222820331590, PBot)
    async def reset_stock_limit(self, ctx, target_address: str):
        # Connect to the P3addr.db database
        p3addr_conn = sqlite3.connect("P3addr.db")

        # Get the user_id associated with the target address
        user_id = get_user_id(p3addr_conn, target_address)

        if not user_id:
            await ctx.send("Invalid or unknown P3 address.")
            return

        # Connect to the currency_system database
        currency_conn = sqlite3.connect("currency_system.db")
        cursor = currency_conn.cursor()

        try:
            # Check if the user has any daily stock buy records
            cursor.execute("""
                SELECT *
                FROM user_daily_buys
                WHERE user_id=? AND timestamp >= date('now', '-1 day')
            """, (user_id,))
            stock_limit_records = cursor.fetchall()

            if stock_limit_records:
                # Reset all daily stock buy records for the user
                cursor.execute("""
                    DELETE FROM user_daily_buys
                    WHERE user_id=?
                """, (user_id,))
                currency_conn.commit()

                await ctx.send(f"Successfully reset daily stock buy limit for the user with P3 address {target_address}.")
            else:
                await ctx.send("This user did not reach the daily stock buy limit for any stocks yet.")
        except sqlite3.Error as e:
            await ctx.send(f"An error occurred: {str(e)}")
        finally:
            # Close the database connections
            p3addr_conn.close()
            currency_conn.close()

    @commands.command(name="reset_stock_limit_all", help="Reset daily stock buy limits for all users.")
    @is_allowed_user(930513222820331590, PBot)
    async def reset_stock_limit_all(self, ctx):
        # Connect to the P3addr.db database
        p3addr_conn = sqlite3.connect("P3addr.db")

        # Connect to the currency_system database
        currency_conn = sqlite3.connect("currency_system.db")
        cursor = currency_conn.cursor()

        try:
            # Get all user IDs with daily stock buy records
            cursor.execute("""
                SELECT DISTINCT user_id
                FROM user_daily_buys
                WHERE timestamp >= date('now', '-1 day')
            """)
            users_with_records = cursor.fetchall()

            if users_with_records:
                for user_id, in users_with_records:
                    # Reset daily stock buy records for each user
                    cursor.execute("""
                        DELETE FROM user_daily_buys
                        WHERE user_id=?
                    """, (user_id,))
                    currency_conn.commit()

                await ctx.send("Successfully reset daily stock buy limits for all users.")
            else:
                await ctx.send("No users reached the daily stock buy limit for any stocks yet.")
        except sqlite3.Error as e:
            await ctx.send(f"An error occurred: {str(e)}")
        finally:
            # Close the database connections
            p3addr_conn.close()
            currency_conn.close()

    @commands.command(name="reset_burn_limit", help="Reset a user's daily burn stock limit based on P3 address.")
    @is_allowed_user(930513222820331590, PBot)
    async def reset_burn_limit(self, ctx, target_address: str):
        # Connect to the P3addr.db database
        p3addr_conn = sqlite3.connect("P3addr.db")

        # Get the user_id associated with the target address
        user_id = get_user_id(p3addr_conn, target_address)

        if not user_id:
            await ctx.send("Invalid or unknown P3 address.")
            return

        # Connect to the currency_system database
        currency_conn = sqlite3.connect("currency_system.db")
        cursor = currency_conn.cursor()

        try:
            # Check if the user has a burn history record
            cursor.execute("SELECT * FROM burn_history WHERE user_id=? AND timestamp >= date('now', '-1 day')", (user_id,))
            burn_history_record = cursor.fetchone()

            if burn_history_record:
                # Reset the burn history
                cursor.execute("DELETE FROM burn_history WHERE user_id=?", (user_id,))
                currency_conn.commit()

                await ctx.send(f"Successfully reset burn limit for the user with P3 address {target_address}.")
            else:
                await ctx.send("This user did not reach the daily burn limit yet.")
        except sqlite3.Error as e:
            await ctx.send(f"An error occurred: {str(e)}")
        finally:
            # Close the database connections
            p3addr_conn.close()
            currency_conn.close()

    @commands.command(name="reset_burn_limit_all", help="Reset daily burn stock limits for all users.")
    @is_allowed_user(930513222820331590, PBot)
    async def reset_burn_limit_all(self, ctx):
        # Connect to the P3addr.db database
        p3addr_conn = sqlite3.connect("P3addr.db")

        # Connect to the currency_system database
        currency_conn = sqlite3.connect("currency_system.db")
        cursor = currency_conn.cursor()

        try:
            # Get all user IDs with burn history records in the past day
            cursor.execute("SELECT DISTINCT user_id FROM burn_history WHERE timestamp >= date('now', '-1 day')")
            users_with_burn_history = cursor.fetchall()

            if not users_with_burn_history:
                await ctx.send("No users found with burn history in the past day.")
                return

            # Reset burn history for each user
            for user_id in users_with_burn_history:
                user_id = user_id[0]

                cursor.execute("DELETE FROM burn_history WHERE user_id=?", (user_id,))
                currency_conn.commit()

                # Get the P3 address for notification
                p3_address = get_p3_address(p3addr_conn, user_id)
                await ctx.send(f"Successfully reset burn limit for the user with P3 address {p3_address}.")

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred: {str(e)}")
        finally:
            # Close the database connections
            p3addr_conn.close()
            currency_conn.close()



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
    async def buy_tickets(self, ctx, quantity: int):
        await ctx.message.delete()
        if quantity <= 0:
            await ctx.send("The quantity of tickets to buy should be greater than 0.")
            return

        user_id = ctx.author.id
        user_balance = get_user_balance(self.conn, user_id)
        cost = quantity * ticketPrice

        if user_balance < cost:
            await ctx.send(f"{ctx.author.mention}, you don't have enough µPPN to buy {quantity} tickets. You need {cost} µPPN.")
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

    @commands.command(name="draw_winner", help="🎉 Draw a winner for the raffle! 🎉")
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
                    channel = ctx.guild.get_channel(1162447728190693466)
                    # Add a countdown before announcing the winner
                    count = 10
                    message = await ctx.send(f"🎉 Drawing winner in {count}... 🎉")
                    for i in range(count, 0, -1):
                        await message.edit(content=f"🎉 Drawing winner in {i}... 🎉")
                        await asyncio.sleep(1)
                    await ctx.send(f"🎉 The winner is {winner.mention}! Congratulations! 🎉 Please head over to {channel.mention} to claim your prize.")
                else:
                    await ctx.send(f"⚠️ Error.. Report to {channel.mention} ⚠️")
                break


    @commands.command(name="clear_tickets")
    @is_allowed_user(930513222820331590, PBot)
    async def clear_tickets(self, ctx):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM raffle_tickets")
        self.conn.commit()

        await ctx.send(f"All raffle tickets have been deleted.")



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

            embed.add_field(name=item_name, value=f"Description: {item_description}\nPrice: {item_price} µPPN\nUsable: {'Yes' if is_usable else 'No'}", inline=False)

        await ctx.send(embed=embed)


    @commands.command(name="inventory", help="View your inventory.")
    async def view_inventory(self, ctx):
        user_id = ctx.author.id
        cursor = self.conn.cursor()

        # Fetch user's inventory
        cursor.execute("""
            SELECT items.item_name, items.item_description, inventory.quantity, items.price
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

        total_value = 0  # Initialize total value

        for item in inventory_data:
            item_name = item[0]
            item_description = item[1] or "No description available"
            quantity = item[2]
            item_price = Decimal(item[3])  # Convert the item price to Decimal

            # Calculate the total value for the item
            item_value = item_price * quantity

            total_value += item_value  # Accumulate the total value

            # Format the values with commas
            formatted_quantity = "{:,}".format(quantity)
            formatted_item_value = "{:,.2f}".format(item_value)

            embed.add_field(name=item_name, value=f"Description: {item_description}\nQuantity: {formatted_quantity}\nValue: {formatted_item_value} µPPN", inline=False)

        # Format the total inventory value with commas
        formatted_total_value = "{:,.2f}".format(total_value)

        # Add the total value of the inventory to the embed
        embed.add_field(name="Total Inventory Value", value=f"{formatted_total_value} µPPN", inline=False)

        await ctx.send(embed=embed)

    @commands.command(name="initialize_dates", help="Initialize all users' dates to the default.")
    async def initialize_dates(self, ctx):
        try:
            with self.get_cursor() as cursor:
                cursor.execute("UPDATE item_usage SET timestamp = '2000-01-01 00:00:00'")
            await ctx.send("All users' dates have been initialized to the default.")
        except sqlite3.Error as e:
            await ctx.send(f"An error occurred: {str(e)}")



    @commands.command(name="use", help="Use a usable item to reset limits (e.g., MarketBadge or FireStarter).")
    async def use_item(self, ctx, item_name: str):
        user_id = ctx.author.id
        member = ctx.author
        guild = ctx.guild

        # Check if the item exists and is usable
        item = None
        with self.get_cursor() as cursor:
            cursor.execute("SELECT * FROM items WHERE item_name=? AND is_usable=1", (item_name,))
            item = cursor.fetchone()

        if not item:
            await ctx.send(f"The item '{item_name}' either does not exist or is not usable.")
            return

        # Check if the user has the item in their inventory
        item_info = None
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT i.quantity, i.item_id
                FROM inventory i
                JOIN items it ON i.item_id = it.item_id
                WHERE i.user_id=? AND it.item_name=? AND i.quantity > 0
            """, (user_id, item_name))
            item_info = cursor.fetchone()

        if not item_info:
            await ctx.send(f"You do not have any {item_name} in your inventory.")
            return

        item_quantity, item_id = item_info

        # Check if the user has already used the item today
        last_usage_date = None
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT MAX(timestamp)
                FROM item_usage
                WHERE user_id=? AND item_name=?
            """, (user_id, item_name))
            last_usage_date = cursor.fetchone()

        if last_usage_date and last_usage_date[0]:
            # Extract the timestamp string
            last_usage_date_str = last_usage_date[0]

            # Convert the timestamp string to a datetime object
            last_usage_datetime = datetime.strptime(last_usage_date_str, '%Y-%m-%d %H:%M:%S')
        else:
            # Initialize with a very old date to ensure it doesn't block new users
            last_usage_datetime = datetime(2000, 1, 1)

        # Calculate the time difference
        time_difference = datetime.now() - last_usage_datetime

        # Check if the last usage was within the last 24 hours
        if time_difference.total_seconds() < 24 * 60 * 60:
            await ctx.send(f"You have already used {item_name} today. You can use it again tomorrow.")
            return

        # Determine the usage limit based on the user's roles
        usage_limit = 1  # Default limit for regular users

        for role in member.roles:
            if role.name == "Bronze Pass":
                usage_limit = 2
            elif role.name == "Silver Pass":
                usage_limit = 3
            elif role.name == "Gold Pass":
                usage_limit = 5

        # Check if the user has reached the usage limit
        if item_quantity <= 0 or item_quantity < usage_limit:
            await ctx.send(f"You do not have enough {item_name} to use based on your role's limit.")
            return

        # Perform actions based on the item
        if item_name == "MarketBadge":
            # Reset daily stock buy limits
            await reset_daily_stock_limits(ctx, user_id)
        elif item_name == "FireStarter":
            # Reset daily burn limits
            await reset_daily_burn_limits(ctx, user_id)

        # Update item quantity after use
        with self.get_cursor() as cursor:
            cursor.execute("UPDATE inventory SET quantity = ? WHERE user_id = ? AND item_id = ?", (item_quantity - 1, user_id, item_id))

        # Log item usage with the current date and time
        current_date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT OR REPLACE INTO item_usage (user_id, item_name, timestamp)
                VALUES (?, ?, ?)
            """, (user_id, item_name, current_date_time))

        await ctx.send(f"You have used {item_name} to reset your limits for today.")





    @commands.command(name="buy_item", help="Buy an item from the marketplace.")
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
            await ctx.send(f"{ctx.author.mention}, you do not have enough µPPN to buy these items.")
            return

        new_balance = current_balance - (total_cost + tax_amount)

        # Update the user's balance
        try:
            update_user_balance(self.conn, user_id, new_balance)
        except ValueError as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while updating the user balance. Error: {str(e)}")
            return

        # Check if the item is one of the restricted items
        if item_name in ["FireStarter", "MarketBadge"]:
            # Check if the user is attempting to buy more than one
            if quantity > 1:
                await ctx.send(f"{ctx.author.mention}, you can only buy one {item_name} at a time.")
                return

            # Check if the user already has the item in their inventory
            cursor.execute("SELECT quantity FROM inventory WHERE user_id=? AND item_id=?", (user_id, item_id))
            existing_quantity = cursor.fetchone()

            if existing_quantity is not None and existing_quantity[0] >= 1:
                await ctx.send(f"{ctx.author.mention}, you can only have 1 {item_name} in your inventory.")
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

        await log_item_transaction(self.conn, ctx, "Buy", item_name, quantity, total_cost, tax_amount, new_balance)
        self.conn.commit()

        await ctx.send(f"{ctx.author.mention}, you have successfully bought {quantity} {item_name}. Your new balance is: {new_balance:,.2f} µPPN.")
        if item_name == "Gold":
            await self.goldReserveBooster(ctx, quantity)



    @commands.command(name="sell_item", help="Sell an item from your inventory.")
    async def sell_item(self, ctx, item_name: str, quantity: int):
        user_id = ctx.author.id
        member = ctx.guild.get_member(user_id)
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
        # Calculate the tax amount based on dynamic factors
        tax_percentage = get_tax_percentage(quantity, total_sale_amount)  # Custom function to determine the tax percentage based on quantity and cost
        if has_role(member, bronze_pass) and tax_rate >= 0.15:
            tax_percentage -= role_discount
        fee = total_sale_amount * Decimal(tax_percentage)
        total_sale_amount = total_sale_amount - fee
        total_cost = total_sale_amount
        tax_amount = fee

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
            """, (user_id, item_id, quantity, total_sale_amount, Decimal(fee)))
        except sqlite3.Error as e:
            await ctx.send(f"{ctx.author.mention}, an error occurred while recording the transaction. Error: {str(e)}")
            return

        await log_item_transaction(self.conn, ctx, "Sell", item_name, quantity, total_sale_amount, Decimal(fee), new_balance)
        self.conn.commit()

        await ctx.send(f"{ctx.author.mention}, you have successfully sold {quantity} {item_name}. Your new balance is: {new_balance:,.2f} µPPN.")


## Market Place Tools

    @commands.command(name="adjust_price", help="Adjust the price of an item.")
    @is_allowed_user(930513222820331590, PBot)
    async def adjust_price(self, ctx, item_name: str, new_price: float):
        cursor = self.conn.cursor()

        # Check if the item exists
        cursor.execute("SELECT item_id, price FROM items WHERE item_name=?", (item_name,))
        item = cursor.fetchone()

        if not item:
            await ctx.send(f"The item '{item_name}' does not exist.")
            return

        item_id, old_price = item

        # Update the item's price
        cursor.execute("UPDATE items SET price=? WHERE item_id=?", (new_price, item_id))
        self.conn.commit()

        await ctx.send(f"The price of the item '{item_name}' has been adjusted from {old_price:,.2f} to {new_price:,.2f}.")


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
            await ctx.send(f"The balance for user with ID {user_id} has been adjusted to {new_balance:,.2f} µPPN.")
        except Exception as e:
            await ctx.send(f"An error occurred while adjusting the balance: {str(e)}")


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

            if available_supply is not None:
                if available_supply < 0:
                    await ctx.send("Invalid available supply value. The available supply must be non-negative.")
                    return
                cursor.execute("UPDATE stocks SET available = ? WHERE symbol = ?", (available_supply, stock_name))

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
        tax_percentage = 0.15
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

        embed.add_field(name="Stock Price", value=f"{stock_price:,.2f} µPPN", inline=True)
        embed.add_field(name="Subtotal", value=f"{subtotal:,.2f} µPPN", inline=True)
        embed.add_field(name=f"Tax (at {tax_percentage*100:.1f}%)", value=f"{tax:,.2f} µPPN", inline=True)
        embed.add_field(name="Total", value=f"{total:,.2f} µPPN", inline=True)
        embed.add_field(name="Potential Stock Price After Buy", value=f"{potential_stock_price:,.2f} µPPN", inline=False)

        await ctx.send(embed=embed)

    @commands.command(name="simulate_sell")
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
        embed.add_field(name="Stock Price", value=f"{stock_price:,.2f} µPPN", inline=True)
        embed.add_field(name="Subtotal", value=f"{subtotal:,.2f} µPPN", inline=True)
        embed.add_field(name=f"Tax (at {tax_percentage*100:.1f}%)", value=f"{tax:,.2f} µPPN", inline=True)
        embed.add_field(name="Total After Tax", value=f"{total:,.2f} µPPN", inline=True)
        embed.add_field(name="Potential Stock Price After Sale", value=f"{potential_stock_price:,.2f} µPPN", inline=True)

        await ctx.send(embed=embed)

    @commands.command(name='roulette', help='Play roulette. Choose a color (red/black/green) or a number (0-36) or "even"/"odd" and your bet amount.')
    async def roulette(self, ctx, choice: str, bet: int):
        user_id = ctx.author.id
        current_balance = get_user_balance(self.conn, user_id)


        # Check if bet amount is positive
        if bet <= 0:
            await ctx.send(f"{ctx.author.mention}, bet amount must be a positive number.")

        if bet < minBet:
            await ctx.send(f"{ctx.author.mention}, minimum bet is {minBet:,.2f} µPPN.")
            return

        if bet > maxBet:
            await ctx.send(f"{ctx.author.mention}, the maximum bet amount is {maxBet:,.2f} µPPN.")
            return
        if bet > current_balance:
            # Calculate the missing amount needed to complete the transaction including tax.
            missing_amount = bet - current_balance
            await ctx.send(f"{ctx.author.mention}, you do not have enough µPPN to place the bet. You need {missing_amount:,.2f} more µPPN, including tax, to place this bet.")
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
            await ctx.send(f"{ctx.author.mention}, you don't have enough µPPN to cover the bet and tax.")
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
            await ctx.send(f"{ctx.author.mention}, congratulations! The ball landed on {spin_result} ({spin_color}). You won {win_amount} µPPN. Your new balance is {new_balance:,.2f}.")
            await log_gambling_transaction(ledger_conn, ctx, "Roulette", bet, f"You won {win_amount} µPPN", new_balance)
            await self.casinoTool(ctx, True)
        else:
            await ctx.send(f"{ctx.author.mention}, the ball landed on {spin_result} ({spin_color}). You lost {total_cost} µPPN including tax. Your new balance is {new_balance:,.2f}.")
            await log_gambling_transaction(ledger_conn, ctx, "Roulette", bet, f"You lost {total_cost} µPPN", new_balance)
            await self.casinoTool(ctx, False)


    @commands.command(name='coinflip', help='Flip a coin and bet on heads or tails.')
    async def coinflip(self, ctx, choice: str, bet: int):
        user_id = ctx.author.id
        current_balance = get_user_balance(self.conn, user_id)

        # Check if bet amount is positive
        if bet <= 0:
            await ctx.send(f"{ctx.author.mention}, bet amount must be a positive number.")
            return

        if bet < minBet:
            await ctx.send(f"{ctx.author.mention}, minimum bet is {minBet:,.2f} µPPN.")
            return

        if bet > maxBet:
            await ctx.send(f"{ctx.author.mention}, the maximum bet amount is {maxBet:,.2f} µPPN.")
            return

        if bet > current_balance:
            # Calculate the missing amount needed to complete the transaction including tax.
            missing_amount = bet - current_balance
            await ctx.send(f"{ctx.author.mention}, you do not have enough µPPN to place the bet. You need {missing_amount:,.2f} more µPPN, including tax, to place this bet.")
            return

        # Flip the coin
        coin_result = random.choice(['heads', 'tails'])

        tax_percentage = get_tax_percentage(bet, current_balance)
        tax = (bet * Decimal(tax_percentage)) * Decimal(0.25)
        total_cost = bet + tax

        # Check for negative balance after tax
        if current_balance - total_cost < 0:
            await ctx.send(f"{ctx.author.mention}, you don't have enough µPPN to cover the bet and tax.")
            return

        # Deduct bet and tax from user's current balance
        new_balance = current_balance - total_cost
        update_user_balance(self.conn, user_id, new_balance)

        # Check if the user wins
        win = choice.lower() == coin_result

        # Create an embed to display the result
        embed = discord.Embed(title="Coinflip Result", color=discord.Color.gold())
        embed.add_field(name="Your Choice", value=choice.capitalize(), inline=True)
        embed.add_field(name="Coin Landed On", value=coin_result.capitalize(), inline=True)

        if win:
            win_amount = bet * 2  # Payout for correct choice is 2x
            new_balance += win_amount
            embed.add_field(name="Congratulations!", value=f"You won {win_amount:,.2f} µPPN", inline=False)
            await self.casinoTool(ctx, True)
        else:
            embed.add_field(name="Oops!", value=f"You lost {total_cost:,.2f} µPPN including tax", inline=False)
            await self.casinoTool(ctx, False)

        embed.add_field(name="New Balance", value=f"{new_balance:,.2f} µPPN", inline=False)

        await ctx.send(embed=embed)
        await log_gambling_transaction(ledger_conn, ctx, "Coinflip", bet, f"{'Win' if win else 'Loss'}", new_balance)


    @commands.command(name='slotmachine', aliases=['slots'], help='Play the slot machine. Bet an amount up to 500,000 µPPN.')
    async def slotmachine(self, ctx, bet: int):
        user_id = ctx.author.id
        current_balance = get_user_balance(self.conn, user_id)

        # Check if bet amount is positive and within the limit
        if bet <= 0:
            await ctx.send(f"{ctx.author.mention}, bet amount must be a positive number.")
            return

        if bet < minBet:
            await ctx.send(f"{ctx.author.mention}, minimum bet is {minBet:,.2f} µPPN.")
            return

        if bet > maxBet:
            await ctx.send(f"{ctx.author.mention}, the maximum bet amount is {maxBet:,.2f} µPPN.")
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
            await ctx.send(f"{ctx.author.mention}, you don't have enough µPPN to cover the bet and tax.")
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
            embed.add_field(name="Congratulations!", value=f"You won {win_amount} µPPN!", inline=False)
            await log_gambling_transaction(ledger_conn, ctx, "Slots", bet, f"You won {win_amount} µPPN", new_balance)
            await self.casinoTool(ctx, True)
        else:
            embed.add_field(name="Better luck next time!", value=f"You lost {total_cost} µPPN including tax. Your new balance is {new_balance:,.2f} µPPN.", inline=False)
            await log_gambling_transaction(ledger_conn, ctx, "Slots", bet, f"You lost {total_cost} µPPN", new_balance)
            await self.casinoTool(ctx, False)

        embed.set_footer(text=f"Your new balance: {new_balance:,.2f} µPPN")
        await ctx.send(embed=embed)


    @commands.command(name='blackjack', aliases=['bj'], help='Play Blackjack against the dealer.')
    async def blackjack(self, ctx, bet: int):
        user_id = ctx.author.id
        current_balance = get_user_balance(self.conn, user_id)

        # Check if bet amount is positive and within the limit
        if bet <= 0:
            await ctx.send(f"{ctx.author.mention}, bet amount must be a positive number.")
            return

        if bet < minBet:
            await ctx.send(f"{ctx.author.mention}, minimum bet is {minBet:,.2f} µPPN.")
            return

        if bet > maxBet:
            await ctx.send(f"{ctx.author.mention}, the maximum bet amount is {maxBet:,.2f} µPPN.")
            return

        # Define Blackjack deck
        deck = ["2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A"]
        suits = ["♠️", "♥️", "♦️", "♣️"]
        cards = [f"{value}{suit}" for value in deck for suit in suits]

        # Shuffle the deck
        random.shuffle(cards)

        # Deal initial cards to the player and dealer
        player_hand = [cards.pop(), cards.pop()]
        dealer_hand = [cards.pop(), cards.pop()]

        # Calculate initial hand values
        player_value = calculate_hand_value(player_hand)
        dealer_value = calculate_hand_value(dealer_hand)

        # Create and send the embed with the initial Blackjack state
        embed = discord.Embed(title="Blackjack", color=discord.Color.green())
        embed.add_field(name="Your Hand", value=", ".join(player_hand) + f" ({player_value})", inline=False)
        embed.add_field(name="Dealer's Hand", value=f"{dealer_hand[0]}, ?")
        embed.set_footer(text=f"Current bet: {bet} µPPN | Your balance: {current_balance:,.2f} µPPN")
        await ctx.send(embed=embed)

        # Check for natural Blackjack (21 with two cards)
        if player_value == 21 and dealer_value != 21:
            await handle_blackjack_win(ctx, user_id, bet, current_balance)
            return
        elif dealer_value == 21 and player_value != 21:
            await handle_blackjack_loss(ctx, user_id, bet, current_balance)
            return
        elif player_value == 21 and dealer_value == 21:
            await handle_blackjack_push(ctx, user_id, bet, current_balance)
            return

        # Ask the player to hit or stand
        await ctx.send(f"{ctx.author.mention}, do you want to hit or stand? (Type `hit` or `stand`)")

        def check(msg):
            return msg.author.id == user_id and msg.content.lower() in ['hit', 'stand']

        while True:
            try:
                response = await self.bot.wait_for('message', check=check, timeout=60)
            except asyncio.TimeoutError:
                await ctx.send(f"{ctx.author.mention}, you took too long to respond. The game has ended.")
                return

            action = response.content.lower()

            if action == 'hit':
                # Deal another card to the player
                player_hand.append(cards.pop())
                player_value = calculate_hand_value(player_hand)

                # Check for bust
                if player_value > 21:
                    await handle_blackjack_loss(ctx, bet, current_balance)
                    return

                # Send updated hand to the player
                embed = discord.Embed(title="Blackjack", color=discord.Color.green())
                embed.add_field(name="Your Hand", value=", ".join(player_hand) + f" ({player_value})", inline=False)
                embed.add_field(name="Dealer's Hand", value=f"{dealer_hand[0]}, ?")
                embed.set_footer(text=f"Current bet: {bet} µPPN | Your balance: {current_balance:,.2f} µPPN")
                await ctx.send(embed=embed)

            elif action == 'stand':
                # Dealer's turn
                while dealer_value < 17:
                    dealer_hand.append(cards.pop())
                    dealer_value = calculate_hand_value(dealer_hand)

                # Check for dealer bust
                if dealer_value > 21:
                    await handle_blackjack_win(ctx, user_id, bet, current_balance)
                    return

                # Determine the winner
                if player_value > dealer_value:
                    await handle_blackjack_win(ctx, user_id, bet, current_balance)
                elif player_value < dealer_value:
                    await handle_blackjack_loss(ctx, user_id, bet, current_balance)
                else:
                    await handle_blackjack_push(ctx, user_id, bet, current_balance)

                return




    @commands.command(name='simulate_roulette', help='Simulate a roulette bet to see the amount in taxes it would cost, and how much you would win/lose.')
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

        await ctx.send(f"{ctx.author.mention}, if you bet {bet} µPPN, here are the hypothetical outcomes:\n"
                       f"- Tax cost: {tax} µPPN\n"
                       f"- Total cost including tax: {total_cost} µPPN\n"
                       f"- Winnings if you chose the correct color: {color_win} µPPN\n"
                       f"- Winnings if you chose the correct number: {number_win} µPPN.")


    @commands.command(name='stats', aliases=['portfolio'], help='Displays the user\'s financial stats.')
    async def stats(self, ctx):
        user_id = ctx.author.id
        P3Addr = generate_crypto_address(ctx.author.id)
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

            # Calculate total buys, sells, and profits/losses from p3ledger
            ledger_conn = sqlite3.connect("p3ledger.db")
            ledger_cursor = ledger_conn.cursor()

            # Get total buys and sells for stocks
            ledger_cursor.execute("""
                SELECT action, SUM(quantity), SUM(price)
                FROM stock_transactions
                WHERE user_id=? AND (action='Buy Stock' OR action='Sell Stock')
                GROUP BY action
            """, (user_id,))
            stock_transactions_data = ledger_cursor.fetchall()

            total_stock_buys = total_stock_sells = total_stock_profits_losses = 0

            for action, quantity, total_price in stock_transactions_data:
                if "Buy" in action:
                    total_stock_buys += total_price
                elif "Sell" in action:
                    total_stock_sells += total_price

            total_stock_profits_losses = total_stock_sells - total_stock_buys

            # Get total buys and sells for ETFs
            ledger_cursor.execute("""
                SELECT action, SUM(quantity), SUM(price)
                FROM stock_transactions
                WHERE user_id=? AND (action='Buy ETF' OR action='Sell ETF' OR action='Buy ALL ETF' OR action='Sell ALL ETF')
                GROUP BY action
            """, (user_id,))
            etf_transactions_data = ledger_cursor.fetchall()

            total_etf_buys = total_etf_sells = total_etf_profits_losses = 0

            for action, quantity, total_price in etf_transactions_data:
                if "Buy" in action:
                    total_etf_buys += total_price
                elif "Sell" in action:
                    total_etf_sells += total_price

            total_etf_profits_losses = total_etf_sells - total_etf_buys

            # Create the embed
            embed = Embed(title=f"{P3Addr} Financial Stats", color=Colour.green())
            embed.add_field(name="Balance", value=f"{current_balance:,.0f} µPPN", inline=False)
            embed.add_field(name="Total Stock Value", value=f"{total_stock_value:,.0f} µPPN", inline=False)
            embed.add_field(name="Total ETF Value", value=f"{total_etf_value:,.0f} µPPN", inline=False)
            embed.add_field(name="Total Funds Value", value=f"{total_funds_value:,.0f} µPPN", inline=False)
            embed.add_field(name="Stock Profits/Losses", value=f"{total_stock_profits_losses:,.0f} µPPN", inline=False)
            embed.add_field(name="ETF Profits/Losses", value=f"{total_etf_profits_losses:,.0f} µPPN", inline=False)

            await ctx.send(embed=embed)

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
        finally:
            # Close the database connections
            conn.close()
            ledger_conn.close()




    @commands.command(name='check_stocks', help='Check which stocks you can still buy.')
    async def check_stocks(self, ctx):
        user_id = ctx.author.id
        member = ctx.guild.get_member(user_id)
        if has_role(member, bronze_pass):
            dStockLimit = 150000000 * 1.25
        else:
            dStockLimit = 150000000

        # Connect to the database
        conn = sqlite3.connect("currency_system.db")
        cursor = conn.cursor()

        # Fetch all stock symbols and their available amounts
        cursor.execute("SELECT symbol, available FROM stocks")
        all_stocks = cursor.fetchall()

        stocks_can_buy = {}
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




    @commands.command(name="burn_stocks", aliases=["burn"], help="Burn a certain amount of stocks to reduce total supply.")
    async def burn_stocks(self, ctx, stock_name: str, amount: int):
        user_id = ctx.author.id

        if stock_name.lower() == "p3:stable":
            await ctx.send("Cannot burn Stable Token P3:Stable")
            return

        if stock_name.lower() == "titanforge":
            await ctx.send("Cannot burn Utility Token TitanForge")
            return

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

        # Check the user's burn history
        if not can_burn_stocks(ctx, cursor, user_id):
            await ctx.send("You have reached the daily limit(5) for stock burns. Please wait until tomorrow.")
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

        # Update the user's burn history
        update_burn_history(cursor, user_id)

        self.conn.commit()

        cost = amount * price_before_burn
        await ctx.send(f"Burned {amount} {stock_name} stocks, reducing the total supply.")
        await log_transaction(ledger_conn, ctx, "Buy Stock", stock_name, amount, cost, cost, 0, 0, price_before_burn)




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


    @commands.command(name="create_swap_order", help="Place a swap order to exchange stocks.")
    @is_allowed_user(930513222820331590, PBot)
    async def swap_stocks(self, ctx, stock1_name: str, amount1: int, stock2_name: str, amount2: int):
        user_id = ctx.author.id
        cursor = self.conn.cursor()

        # Fetch user's stock holdings
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

        # Place swap order in the database
        cursor.execute("INSERT INTO swap_orders (user_id, stock1, amount1, stock2, amount2, status) VALUES (?, ?, ?, ?, ?, 'open')", (user_id, stock1_name, amount1, stock2_name, amount2))
        self.conn.commit()

        await ctx.send(f"Swap order placed. The system will attempt to match your order with another user's order for a fair trade.")

    @commands.command(name="open_swap_orders", help="View all open swap orders.")
    @is_allowed_user(930513222820331590, PBot)
    async def open_swap_orders(self, ctx):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM swap_orders WHERE status='open'")
        orders = cursor.fetchall()

        if orders:
            order_list = "\n".join([f"Order ID: {order[0]}, User: {generate_crypto_address(order[1])}, Stock 1: {order[2]}, Amount 1: {order[3]}, Stock 2: {order[4]}, Amount 2: {order[5]}" for order in orders])
            await ctx.send(f"Open Swap Orders:\n{order_list}")
        else:
            await ctx.send("No open swap orders.")

    @commands.command(name="my_swap_orders", help="View your own open swap orders.")
    @is_allowed_user(930513222820331590, PBot)
    async def my_swap_orders(self, ctx):
        user_id = ctx.author.id
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM swap_orders WHERE user_id=? AND status='open'", (user_id,))
        orders = cursor.fetchall()

        if orders:
            order_list = "\n".join([f"Order ID: {order[0]}, Stock 1: {order[2]}, Amount 1: {order[3]}, Stock 2: {order[4]}, Amount 2: {order[5]}" for order in orders])
            await ctx.send(f"Your Open Swap Orders:\n{order_list}")
        else:
            await ctx.send("You have no open swap orders.")

    @commands.command(name="close_swap_order", help="Close your own open swap order.")
    @is_allowed_user(930513222820331590, PBot)
    async def close_swap_order(self, ctx, order_id: int):
        user_id = ctx.author.id
        cursor = self.conn.cursor()

        # Check if the order belongs to the user and is open
        cursor.execute("SELECT * FROM swap_orders WHERE id=? AND user_id=? AND status='open'", (order_id, user_id))
        order = cursor.fetchone()

        if order:
            # Close the order
            cursor.execute("UPDATE swap_orders SET status='matched' WHERE id=?", (order_id,))
            self.conn.commit()
            await ctx.send(f"Swap order {order_id} closed successfully.")
        else:
            await ctx.send("The specified order does not exist or is not open.")

    @commands.command(name="match_swap_order", help="Attempt to match open swap orders.")
    @is_allowed_user(930513222820331590, PBot)
    async def match_swap_order(self, ctx):
        user_id = ctx.author.id
        cursor = self.conn.cursor()

        try:
            # Fetch user's open swap orders
            cursor.execute("SELECT * FROM swap_orders WHERE user_id=? AND status='open'", (user_id,))
            user_orders = cursor.fetchall()

            if not user_orders:
                await ctx.send("You have no open swap orders.")
                return

            # Fetch all other open swap orders that are compatible with the user's orders
            compatible_orders = []
            for user_order in user_orders:
                stock1, amount1, stock2, amount2 = user_order[2], user_order[3], user_order[4], user_order[5]
                cursor.execute("""
                    SELECT *
                    FROM swap_orders
                    WHERE status='open'
                    AND ((stock1=? AND amount1<=? AND stock2 IN (SELECT stock1 FROM swap_orders WHERE status='open' AND stock2=? AND amount2<=?))
                        OR (stock2=? AND amount2<=? AND stock1 IN (SELECT stock2 FROM swap_orders WHERE status='open' AND stock1=? AND amount1<=?)))
                """, (stock1, amount1, stock2, amount2, stock2, amount2, stock1, amount1))
                compatible_orders.extend(cursor.fetchall())

            if not compatible_orders:
                await ctx.send("No compatible open swap orders found.")
                return

            # Match orders and execute swaps
            for compatible_order in compatible_orders:
                matched_order_id = compatible_order[0]

                # Close the user's order
                cursor.execute("UPDATE swap_orders SET status='matched' WHERE id=?", (matched_order_id,))

                # Close the matched order
                cursor.execute("UPDATE swap_orders SET status='matched' WHERE id=?", (user_order[0],))

                # Execute the stock swap (placeholder logic, update based on your implementation)
                self.execute_stock_swap(user_order, compatible_order)

                self.conn.commit()

            await ctx.send("Swap orders matched successfully.")

        except sqlite3.Error as e:
            # Handle database errors
            print(f"Database error: {e}")
            await ctx.send("An error occurred while processing the swap orders. Please try again later.")

        except Exception as e:
            # Handle unexpected errors
            print(f"An unexpected error occurred: {e}")
            await ctx.send("An unexpected error occurred. Please try again later.")



## Server stats

    @commands.command(name="server_stats", help="Display server statistics.")
    async def server_stats(self, ctx):
        # Get the amount of servers the bot is a member of
        server_count = len(self.bot.guilds)

        # Get the amount of users with stored P3 addresses
        address_count = await self.get_user_count()

        # Get the size of currency_system.db, p3ledger.db, and P3addr.db
        db_sizes = {
            "currency_system.db": self.get_file_size("currency_system.db"),
            "p3ledger.db": self.get_file_size("p3ledger.db"),
            "P3addr.db": self.get_file_size("P3addr.db"),
        }

        # Get the number of lines in cogs/currency_system_cog.py
        cog_lines = self.get_file_lines("cogs/currency_system_cog.py")

        # Create and send an embed with the information
        embed = discord.Embed(title="Server Statistics", color=discord.Color.green())
        embed.add_field(name="Servers", value=server_count)
        embed.add_field(name="P3 Addresses", value=address_count)
        embed.add_field(name="Lines in currency_system", value=cog_lines)
        for db_name, db_size in db_sizes.items():
            embed.add_field(name=f"Size of {db_name}", value=db_size)

        await ctx.send(embed=embed)

    async def get_user_count(self):
        try:
            P3conn = sqlite3.connect("P3addr.db")
            cursor = P3conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM user_addresses")
            count = cursor.fetchone()[0]
            return count
        except Exception as e:
            print(f"Error getting user count: {e}")
            return "Error"

    def get_file_size(self, filename):
        try:
            size = os.path.getsize(filename)
            return self.format_size(size)
        except Exception as e:
            print(f"Error getting file size for {filename}: {e}")
            return "Error"

    def format_size(self, size):
        # Format size in a human-readable format
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                break
            size /= 1024.0
        return f"{size:,.2f} {unit}"

    def get_file_lines(self, filename):
        try:
            with open(filename, 'r', encoding='utf-8') as file:
                lines = file.readlines()
                return len(lines)
        except Exception as e:
            print(f"Error getting file lines for {filename}: {e}")
            return "Error"


    @commands.command(name="job", help="Work and earn rewards every four hours!")
    async def job(self, ctx):
        user_id = ctx.author.id
        member = ctx.guild.get_member(user_id)
        cooldown_key = f"job_cooldown:{user_id}"

        if has_role(member, bronze_pass) or has_role(member, 1161679929163915325):
            # Check if the user is on cooldown
            last_job_time_str = self.last_job_times.get(user_id)
            if last_job_time_str:
                last_job_time = last_job_time_str
                cooldown_time = timedelta(hours=4)
                remaining_cooldown = last_job_time + cooldown_time - datetime.utcnow()

                if remaining_cooldown.total_seconds() > 0:
                    await ctx.send(f"You can work again in {remaining_cooldown}.")
                    return

            # Fetch the list of available stocks
            cursor = self.conn.cursor()
            cursor.execute("SELECT symbol FROM stocks WHERE available > 0")
            available_stocks = [stock['symbol'] for stock in cursor.fetchall()]

            if not available_stocks:
                await ctx.send("No stocks are available for rewards at the moment. Try again later.")
                return

            # Reward the user
            if random.choice([True, False]):  # 50% chance of not getting any reward
                tokens_reward = random.randint(100, 100000000)
                stocks_reward_symbol = random.choice(available_stocks)
                stocks_reward_amount = random.randint(1, 10000000)

                # Update user balance
                current_balance = get_user_balance(self.conn, user_id)
                new_balance = current_balance + tokens_reward
                update_user_balance(self.conn, user_id, new_balance)

                # Update user stocks
                cursor.execute("UPDATE stocks SET available = available - ? WHERE symbol=?", (stocks_reward_amount, stocks_reward_symbol))
                cursor.execute("INSERT INTO user_stocks (user_id, symbol, amount) VALUES (?, ?, ?) ON CONFLICT(user_id, symbol) DO UPDATE SET amount = amount + ?",
                               (user_id, stocks_reward_symbol, stocks_reward_amount, stocks_reward_amount))

                # Log the transfer
                await log_transfer(ledger_conn, ctx, self.bot.user, user_id, user_id, tokens_reward)
                await log_stock_transfer(ledger_conn, ctx, self.bot.user, ctx.author, stocks_reward_symbol, stocks_reward_amount)

                message = f"🛠️ You've worked hard and earned {tokens_reward} µPPN and {stocks_reward_amount} shares of {stocks_reward_symbol}! 🚀"
            else:
                message = "😴 You worked, but it seems luck wasn't on your side this time. Try again later!"

            # Set the cooldown for the user
            self.last_job_times[user_id] = datetime.utcnow()

            await ctx.send(message)
        else:
            await ctx.send(f'Must have at least the Bronze Pass to use this command, check out the Discord Store Page to Purchase')

async def setup(bot):
    await bot.add_cog(CurrencySystem(bot))
