from discord.ext import commands, tasks
from discord import Embed, Colour, File
from tabulate import tabulate
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from matplotlib.ticker import FuncFormatter
from discord.utils import get
from math import ceil, floor
from sqlite3.dbapi2 import Connection, Cursor
from contextlib import contextmanager
from functools import partial
from typing import Union
from dbutils.pooled_db import PooledDB
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt2
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
from matplotlib.animation import FuncAnimation
from collections import defaultdict
import aiomysql.sa
from aiomysql.sa import create_engine
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
import calendar
import matplotlib
import timeit
import statistics
import aiosqlite
import uuid

matplotlib.use('agg')

# Hardcoded Variables

announcement_channel_ids = [1093540470593962014, 1124784361766650026, 1124414952812326962]
stockMin = 1
baseMinPrice = 1
stockMax = 10000000
dStockLimit = 150000000 #2000000 standard
dETFLimit = 5000000000000
treasureMin = 10000000
treasureMax = 50000000
MAX_BALANCE = Decimal('5000000000000000000')
sellPressureMin = 0.000045
sellPressureMax = 0.000095
buyPressureMin = 0.0000155
buyPressureMax = 0.0001475
stockDecayValue = 0.0000125
decayMin = 0.01
resetQSE = 100
dailyMin = 100000
dailyMax = 500000
ticketPrice = 100
maxTax = 0.50
minBet = 10000
maxBet = 500000000
last_buy_time = {}
last_sell_time = {}
user_transactions = {}
user_locks = {}
inverseStocks = ["ContrarianCraze", "PolyInverse", "LOL"]







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

def setup_ledger():
    try:
        # Create a connection to the SQLite database with caching and paging
        conn = sqlite3.connect("p3ledger.db", check_same_thread=False, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA cache_size = 10000")  # Set cache size (adjust as needed)
        conn.execute("PRAGMA page_size = 4096")   # Set page size (adjust as needed)
        conn.execute("PRAGMA synchronous=NORMAL;")
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
                price NUMERIC(20, 10) NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Add an index on user_id and symbol for faster retrieval
        cursor.execute("""
            CREATE INDEX idx_symbol_action_timestamp
            ON stock_transactions (symbol, action, timestamp);
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
        cursor.execute("""
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
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA cache_size = 10000")  # Set cache size (adjust as needed)
    conn.execute("PRAGMA page_size = 4096")   # Set page size (adjust as needed)
    conn.execute("PRAGMA synchronous=NORMAL;")

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
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA cache_size = 10000")  # Set cache size (adjust as needed)
    conn.execute("PRAGMA page_size = 4096")   # Set page size (adjust as needed)
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Add available column to stocks table if not exists
    cursor.execute("PRAGMA table_info(stocks)")
    columns = cursor.fetchall()
    if 'available' not in [column[1] for column in columns]:
        cursor.execute("ALTER TABLE stocks ADD COLUMN available INT")

    # Create 'users' table
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                balance BLOB NOT NULL
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id);")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'users' table: {e}")

    # Create 'raffle_tickets' table
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS raffle_tickets (
                user_id INTEGER PRIMARY KEY,
                quantity INTEGER,
                timestamp INTEGER
            )
        ''')
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_raffle_tickets_user_id ON raffle_tickets(user_id);")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'raffle_tickets' table: {e}")

    # Create 'stocks' table
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                symbol TEXT PRIMARY KEY,
                available INTEGER64 NOT NULL,
                price NUMERIC(20, 10) NOT NULL,
                QSE_required INTEGER NOT NULL,
                QSE_rewarded INTEGER NOT NULL
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stocks_symbol ON stocks(symbol);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stocks_price ON stocks(price);")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'stocks' table: {e}")

    # Create 'user_stocks' table
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_stocks (
                user_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                amount REAL NOT NULL,
                action TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                FOREIGN KEY (symbol) REFERENCES stocks(symbol),
                PRIMARY KEY (user_id, symbol)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_stocks_user_id ON user_stocks(user_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_stocks_symbol ON user_stocks(symbol);")
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
                FOREIGN KEY (symbol) REFERENCES stocks(symbol),
                CREATE INDEX idx_limit_orders_symbol_order_type_price_created_at
                ON limit_orders (symbol, order_type, price, created_at);

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

    try:
        # Create Users table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_wallets (
                user_id INTEGER PRIMARY KEY,
                wallet_address TEXT NOT NULL,
                p3_address TEXT NOT NULL
            )
        """)
        print("WalletConnect created successfully")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the tables: {str(e)}")

    try:
        # Create user_stakes table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_stakes (
                user_id INTEGER,
                nft TEXT NOT NULL,
                tokenid TEXT NOT NULL,
                stake_timestamp TEXT,
                PRIMARY KEY (user_id, nft, tokenid),
                FOREIGN KEY (user_id) REFERENCES user_wallets (user_id)
            )
        """)
        print("user_stakes table created successfully")

    except sqlite3.Error as e:
        print(f"An error occurred while creating the user_stakes table: {e}")

    try:

        # Create a table to store user deposits
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS deposits (
                user_id INTEGER,
                lock_duration INTEGER,
                amount REAL,
                interest_rate REAL,
                start_date TIMESTAMP,
                PRIMARY KEY (user_id, lock_duration)
            );
        ''')

    except sqlite3.Error as e:
        print(f"An error occurred: {str(e)}")

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users_level (
                user_id INTEGER PRIMARY KEY,
                level INTEGER NOT NULL DEFAULT 1,
                experience INTEGER NOT NULL DEFAULT 0
            )
        """)
    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'user_level' table: {e}")

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users_rpg_stats (
                user_id INTEGER PRIMARY KEY,
                cur_hp INTEGER NOT NULL DEFAULT 10,
                max_hp INTEGER NOT NULL DEFAULT 10,
                atk INTEGER NOT NULL DEFAULT 1,
                def INTEGER NOT NULL DEFAULT 1,
                eva INTEGER NOT NULL DEFAULT 1,
                luck INTEGER NOT NULL DEFAULT 1,
                chr INTEGER NOT NULL DEFAULT 1,
                spd INTEGER NOT NULL DEFAULT 1
            )
        """)
    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'user_stats' table: {e}")




    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users_rpg_metrics (
                user_id INTEGER PRIMARY KEY,
                kills INTEGER NOT NULL DEFAULT 0,
                deaths INTEGER NOT NULL DEFAULT 0,
                heals INTEGER NOT NULL DEFAULT 0,
                healed INTEGER NOT NULL DEFAULT 0
            )
        """)
    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'user_rpg_metrics' table: {e}")


    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users_rpg_inventory (
                user_id INTEGER PRIMARY KEY,
                firearm TEXT NOT NULL DEFAULT None,
                ammo TEXT NOT NULL DEFAULT None,
                bodyarmor TEXT NOT NULL DEFAULT None,
                medpack TEXT NOT NULL DEFAULT None
            )
        """)
    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'user_rpg_inventory' table: {e}")


    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users_rpg_cities (
                user_id INTEGER PRIMARY KEY,
                current_city TEXT NOT NULL DEFAULT 'StellarHub',
                last_city TEXT NOT NULL DEFAULT None,
                traveling TEXT NOT NULL DEFAULT 'No',
                timestamp TIMESTAMP NOT NULL DEFAULT None
            )
        """)
    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'user_rpg_cities' table: {e}")


    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_city_stats (
                city TEXT PRIMARY KEY,
                QSE TEXT NOT NULL DEFAULT 0,
                Resources INTEGER NOT NULL DEFAULT 0,
                Stocks INTEGER NOT NULL DEFAULT 0,
                ETPs INTEGER NOT NULL DEFAULT 0
            )
        """)
    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'user_city_stats' table: {e}")


    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS updown_orders (
                user_id INTEGER NULL DEFAULT None,
                asset TEXT NOT NULL DEFAULT None,
                current_price INTEGER NOT NULL DEFAULT None,
                lower_limit INTEGER NOT NULL DEFAULT None,
                upper_limit INTEGER NOT NULL DEFAULT None,
                contract_date TIMESTAMP NOT NULL DEFAULT None,
                expiration TIMESTAMP NOT NULL DEFAULT None,
                order_id INTEGER NOT NULL DEFAULT None
            )
        """)
    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'updown_orders' table: {e}")


    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS futures_orders (
                order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                order_type TEXT NOT NULL,  -- 'buy' or 'sell'
                price REAL NOT NULL,
                quantity INTEGER NOT NULL,
                expiration TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (symbol) REFERENCES stocks(symbol)
            )
        """)
    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'futures_orders' table: {e}")



    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS market_value (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                MV TEXT NOT NULL

            );
        """)

        print("Tables created successfully")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the tables: {str(e)}")

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reserve_value (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                qse TEXT NOT NULL,
                stocks TEXT NOT NULL,
                total TEXT NOT NULL

            );
        """)

        print("Tables created successfully")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the tables: {str(e)}")

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS banks (
                bank_id INTEGER PRIMARY KEY AUTOINCREMENT,
                bank_name TEXT NOT NULL,
                qse_stored REAL NOT NULL,
                stock_value_stored REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

        """)
        cursor.execute("""CREATE INDEX IF NOT EXISTS idx_banks_bank_name ON banks (bank_name);""")
        print("Banks created successfully")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the tables: {str(e)}")

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_accounts (
                account_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                bank_id INTEGER NOT NULL,
                qse_balance REAL NOT NULL,
                stock_assets TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (bank_id) REFERENCES banks(bank_id)
            );


        """)
        cursor.execute("""CREATE INDEX IF NOT EXISTS idx_user_accounts_user_id ON user_accounts (user_id);""")
        print("User Account Banks created successfully")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the tables: {str(e)}")


    conn.commit()
    return conn




setup_database()
setup_ledger()
create_p3addr_table()
create_vip_table()



## Bank Engine



async def create_bank(self, bank_name):
    try:
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO banks (bank_name, qse_stored, stock_value_stored)
            VALUES (?, 0.0, 0.0)
        """, (bank_name,))
        self.conn.commit()
        return f"Bank '{bank_name}' created successfully."
    except sqlite3.Error as e:
        return f"Error creating bank: {e}"

async def show_all_banks(self):
    try:
        cursor = self.conn.cursor()
        cursor.execute("SELECT bank_id, bank_name FROM banks")
        banks = cursor.fetchall()

        if banks:
            print("Banks:")
            for bank_id, bank_name in banks:
                print(f"Bank ID: {bank_id}, Bank Name: {bank_name}")
        else:
            print("No banks found.")
    except sqlite3.Error as e:
        print(f"An error occurred while retrieving banks: {str(e)}")

async def create_user_account(self, ctx, bank_id):
    try:
        cursor = self.conn.cursor()
        user_id = ctx.author.id

        # Check if the user already has an account
        cursor.execute("SELECT * FROM user_accounts WHERE user_id = ? AND bank_id = ?", (user_id, bank_id))
        existing_account = cursor.fetchone()

        if existing_account:
            return "User already has an account."

        # If no existing account, create a new one
        cursor.execute("""
            INSERT INTO user_accounts (user_id, bank_id, qse_balance, stock_assets)
            VALUES (?, ?, 0.0, '')
        """, (user_id, bank_id))

        self.conn.commit()
        return "User account created successfully."

    except sqlite3.Error as e:
        return f"Error creating user account: {e}"

async def deposit_qse(self, ctx, bank_id, amount):
    try:
        cursor = self.conn.cursor()
        user_id = ctx.author.id

        # Check if the user already has an account
        cursor.execute("SELECT * FROM user_accounts WHERE user_id = ? AND bank_id = ?", (user_id, bank_id))
        existing_account = cursor.fetchone()

        if existing_account:

            cursor.execute("""
                UPDATE user_accounts
                SET qse_balance = qse_balance + ?
                WHERE user_id = ? AND bank_id = ?
            """, (amount, user_id, bank_id))
            self.conn.commit()
            return f"Deposited {amount:,.0f} QSE successfully."
        else:
            return f"No account found for this bank"
    except sqlite3.Error as e:
        return f"Error depositing QSE: {e}"

async def deposit_stock_assets(self, ctx, bank_id, assets):
    try:
        cursor = self.conn.cursor()
        user_id = ctx.author.id
        cursor.execute("""
            UPDATE user_accounts
            SET stock_assets = ?
            WHERE user_id = ? AND bank_id = ?
        """, (assets, user_id, bank_id))
        self.conn.commit()
        return "Deposited stock assets successfully."
    except sqlite3.Error as e:
        return f"Error depositing stock assets: {e}"

async def withdraw_qse(self, ctx, bank_id, amount):
    try:
        cursor = self.conn.cursor()
        user_id = ctx.author.id
        # Check if the user already has an account
        cursor.execute("SELECT * FROM user_accounts WHERE user_id = ? AND bank_id = ?", (user_id, bank_id))
        existing_account = cursor.fetchone()

        if existing_account:
            current_qse_balance = existing_account[3]

            # Check if the withdrawal amount is greater than the current balance
            if amount > current_qse_balance:
                return "Withdrawal amount exceeds the current QSE balance. Cannot withdraw."

            # Update the QSE balance
            cursor.execute("""
                UPDATE user_accounts
                SET qse_balance = qse_balance - ?
                WHERE user_id = ? AND bank_id = ?
            """, (amount, user_id, bank_id))
            self.conn.commit()
            await self.send_from_reserve(ctx, ctx.author.id, amount)
            return f"Withdrew {amount:,.0f} QSE successfully."
        else:
            return "No account found for this bank"
    except sqlite3.Error as e:
        return f"Error withdrawing QSE: {e}"

async def withdraw_stock_assets(self, ctx, bank_id):
    try:
        cursor = self.conn.cursor()
        user_id = ctx.author.id
        cursor.execute("""
            UPDATE user_accounts
            SET stock_assets = ''
            WHERE user_id = ? AND bank_id = ?
        """, (user_id, bank_id))
        self.conn.commit()
        return "Withdrawn stock assets successfully."
    except sqlite3.Error as e:
        return f"Error withdrawing stock assets: {e}"

async def view_account(self, ctx, bank_id):
    try:
        cursor = self.conn.cursor()
        user_id = ctx.author.id
        cursor.execute("""
            SELECT * FROM user_accounts
            WHERE user_id = ? AND bank_id = ?
        """, (user_id, bank_id))
        account_data = cursor.fetchone()
        if account_data:
            qse_balance = account_data[3]
            formatted_qse_balance = f"{qse_balance:,.0f}"

            embed = Embed(
                title=f"Account Information for User {get_p3_address(self.P3addrConn, user_id)}",
                description=f"Bank: P3:Bank",
                color=0x00ff00
            )
            embed.add_field(name="QSE Balance", value=formatted_qse_balance)
            embed.add_field(name="Stock Assets", value=account_data[4])
            return embed
        else:
            return "User account not found."
    except sqlite3.Error as e:
        return f"Error viewing account: {e}"

async def get_total_qse_deposited(self):
    try:
        cursor = self.conn.cursor()

        # Summing up the QSE balance for all accounts
        cursor.execute("""
            SELECT SUM(qse_balance) FROM user_accounts
        """)
        total_qse_deposited = cursor.fetchone()[0]

        if total_qse_deposited is not None:
            return total_qse_deposited
        else:
            return 0  # Return 0 if there are no accounts with deposited QSE

    except sqlite3.Error as e:
        return f"Error getting total QSE deposited: {e}"


# Set the locale to a valid one (e.g., 'en_US.UTF-8')
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

# Create a formatter function to format prices
def price_formatter(x, pos):
    if x < 0.01:
        # Format small prices differently
        return f"{x:.8f}"
    else:
        return locale.currency(x, grouping=True)


def buy_check(cursor, user_id, stock_name, time_threshold):
    # Convert shorthand time to timedelta
    time_threshold = parse_time_shorthand(time_threshold)

    # Get the total amount bought within the specified time threshold by the user for this stock
    cursor.execute("""
        SELECT SUM(amount), MAX(timestamp)
        FROM user_daily_buys
        WHERE user_id=? AND symbol=? AND timestamp >= ?
    """, (user_id, stock_name, datetime.now() - time_threshold))

    hourly_bought_record = cursor.fetchone()
    hourly_bought = hourly_bought_record[0] if hourly_bought_record and hourly_bought_record[0] is not None else 0
    last_purchase_time_str = hourly_bought_record[1] if hourly_bought_record and hourly_bought_record[1] is not None else None

    # Convert last_purchase_time to a datetime object
    last_purchase_time = datetime.strptime(last_purchase_time_str, "%Y-%m-%d %H:%M:%S") if last_purchase_time_str else None

    remaining_time = calculate_remaining_time(last_purchase_time, time_threshold)

    return hourly_bought > 0, last_purchase_time, remaining_time

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
                value=f"Total Quantity: {total_quantity}\nTotal Pre-Gas Amount: {total_pre_tax_amount:,.0f} $QSE",
                inline=True
            )
        pages.append(embed)

    return pages




async def tax_command(self, ctx):
    user_id = ctx.author.id
    bot_id = PBot
    base_amount = 1000
    tax_percentage = self.calculate_tax_percentage(ctx, "buy_stock")
    tax_amount = round(base_amount * tax_percentage)
    total_amount = base_amount + tax_amount

    user_balance = get_user_balance(self.conn, user_id)
    bot_balance = get_user_balance(self.conn, bot_id)

    if user_balance < total_amount:
        await ctx.send(f"{ctx.author.mention}, you don't have enough $QSE to run this command. Your current balance is {sender_balance:,.2f} $QSE. Requires {base_amount} + {tax_amount}")
        return

    # Deduct the tax from the sender's balance
    await update_user_balance(self.conn, user_id, user_balance - total_amount)
    await update_user_balance(self.conn, bot_id, bot_balance + total_amount)
    ledger_conn = sqlite3.connect("p3ledger.db")
    await log_transfer(self, ledger_conn, ctx, "P3 Bot", self.bot_address, get_user_id(self.P3addrConn, self.bot_address), total_amount)
    print(f"Address: {get_p3_address(self.P3addrConn, ctx.author.id)} taxed {total_amount:,.2f} for using a taxable command")


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

async def get_stock_price(self, ctx, stock_name):
    avg_buy, avg_sell = await calculate_average_prices_by_symbol(self, stock_name)
    avg_price = (avg_buy + avg_sell) / 2

    return avg_price


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
            P3addrConn = sqlite3.connect("P3addr.db")

            stock_symbols = ', '.join(stock[0] for stock in stocks)
            await ctx.send(f"Successfully reset daily stock buy limits for the user with ID {get_p3_address(P3addrConn ,user_id)} and stocks: {stock_symbols}.")
        else:
            await ctx.send(f"This user did not reach the daily stock buy limit for any stocks yet.")
    except sqlite3.Error as e:
        await ctx.send(f"An error occurred: {str(e)}")
    finally:
        # Close the database connection
        currency_conn.close()










# Economy Engine

async def send_stock(self, ctx, target, sender, symbol: str, amount: int, verbose: bool = True):
    cursor = self.conn.cursor()
    target_user_id = get_user_id(self.P3addrConn, target)
    sender_user_id = get_user_id(self.P3addrConn, sender)
    # Validate P3 address
    if not target.startswith("P3:"):
        await ctx.send("Please provide a valid P3 address.")
        return False

    # Validate amount
    if not isinstance(amount, int) or amount <= 0:
        await ctx.send("Please provide a valid positive integer amount.")
        return False

    # Check if the stock exists.
    cursor.execute("SELECT symbol, available FROM stocks WHERE symbol=?", (symbol,))
    stock = cursor.fetchone()
    if stock is None:
        await ctx.send(f"No stock with symbol {symbol} found.")
        return False

    # Check if there's enough of the stock available in the user's stash.
    cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (sender_user_id, symbol))
    user_stock = cursor.fetchone()
    if user_stock is None or user_stock['amount'] < amount:
        await ctx.send(f"Not enough of {symbol} available in your stash.")
        return False

    try:
        # Deduct the stock from the user's stash.
        new_amount = user_stock['amount'] - amount
        cursor.execute("UPDATE user_stocks SET amount=? WHERE user_id=? AND symbol=?", (new_amount, sender_user_id, symbol))

        # Update the recipient's stocks.
        cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (target_user_id, symbol))
        recipient_stock = cursor.fetchone()
        if recipient_stock is None:
            cursor.execute("INSERT INTO user_stocks(user_id, symbol, amount) VALUES(?, ?, ?)", (target_user_id, symbol, amount))
        else:
            new_amount = recipient_stock['amount'] + amount
            cursor.execute("UPDATE user_stocks SET amount=? WHERE user_id=? AND symbol=?", (new_amount, target_user_id, symbol))

        self.conn.commit()

        # Log the stock transfer
#        await log_stock_transfer(self, ledger_conn, ctx, sender_user_id, target_user_id, symbol, amount)

        if verbose:
            await ctx.send(f"Sent {amount:,} of {symbol} to {target}.")
        return True

    except Exception as e:
        # Handle exceptions (rollback changes if necessary)
        self.conn.rollback()
        await ctx.send(f"An error occurred during the stock transfer: {e}")
        return False

def remove_order(conn: Connection, order_id: int):
    try:
        with conn:
            conn.execute("DELETE FROM limit_orders WHERE order_id = ?", (order_id,))
        print(f"Order {order_id} removed successfully.")
    except sqlite3.Error as e:
        print(f"An error occurred while removing order {order_id}: {e}")

def update_order_quantity(conn: Connection, order_id: int, new_quantity: int):
    try:
        with conn:
            conn.execute("UPDATE limit_orders SET quantity = :new_quantity WHERE order_id = :order_id",
                         {"new_quantity": new_quantity, "order_id": order_id})
        print(f"Order {order_id} quantity updated to {new_quantity} successfully.")
    except sqlite3.Error as e:
        print(f"An error occurred while updating quantity for order {order_id}: {e}")

async def fetch_sell_orders(self, stock_name):
    cursor = self.conn.cursor()

    # Fetch sell orders for the specified stock
    cursor.execute("""
        SELECT * FROM limit_orders
        WHERE symbol = ? AND order_type = 'sell'
        ORDER BY price ASC, created_at ASC
    """, (stock_name,))
    return cursor.fetchall()


async def fetch_buy_orders(self, stock_name):
    cursor = self.conn.cursor()

    # Fetch sell orders for the specified stock
    cursor.execute("""
        SELECT * FROM limit_orders
        WHERE symbol = ? AND order_type = 'buy'
        ORDER BY price DESC, created_at ASC
    """, (stock_name,))
    return cursor.fetchall()

async def display_transaction_details(ctx, p3addr, stock_name, amount):
    # Fetch relevant details for displaying transaction details
    current_balance = get_user_balance(self.conn, ctx.author.id)
    new_balance = get_user_balance(self.conn, ctx.author.id)

    # Display the transaction details
    color = discord.Color.green()
    embed = discord.Embed(title=f"Stock Transaction Completed", color=color)
    embed.add_field(name="Address:", value=f"{p3addr}", inline=False)
    embed.add_field(name="Stock:", value=f"{stock_name}", inline=False)
    embed.add_field(name="Amount:", value=f"{amount:,.2f}", inline=False)
    embed.add_field(name="New Balance:", value=f"{new_balance:,.2f} $QSE", inline=False)
    embed.set_footer(text=f"Timestamp: {datetime.utcnow()}")

    # Send the transaction details to the user
    await ctx.send(embed=embed)

async def asset_type(self, ctx, symbol):
    cursor = self.conn.cursor()
    ETP = "etp"
    ITEM = "item"
    STOCK = "stock"

    # Check if the item exists in the marketplace
    cursor.execute("SELECT item_id, price, is_usable FROM items WHERE item_name=?", (symbol,))
    item_data = cursor.fetchone()

    # Retrieve stock information
    cursor.execute("SELECT * FROM stocks WHERE symbol=?", (symbol,))
    stock_data = cursor.fetchone()

    if item_data:
        return ITEM
    if stock_data:
        return STOCK


async def swap_item(self, ctx, order_type, symbol, amount):
    try:
        cursor.execute("""
            INSERT INTO inventory (user_id, item_id, quantity)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, item_id) DO UPDATE SET quantity = quantity + ?
        """, (user_id, item_id, quantity, quantity))
    except sqlite3.Error as e:
        await ctx.send(f"{ctx.author.mention}, an error occurred while updating user inventory. Error: {str(e)}")
        return

async def get_item_info(self, ctx, symbol):
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

    # Initialize variables outside the loop
    item_info = None
    total_value = 0

    for item in inventory_data:
        if item[0].lower() == symbol.lower():
            item_name = item[0]
            item_description = item[1] or "No description available"
            quantity = Decimal(item[2])
            item_price = Decimal(item[3])  # Convert the item price to Decimal

            # Calculate the total value for the item
            item_value = item_price * quantity
            total_value += item_value

            # Store the item information in a tuple
            item_info = (item_name, item_description, quantity, item_price, item_value)

    if item_info:
        return item_info, total_value
    else:
        await ctx.send(f"{ctx.author.mention}, the item '{symbol}' was not found in your inventory.")


async def check_smart_contract(self, ctx):
    if self.contract_pool:
        self.is_paused = True
        async with self.transaction_lock:
            print("Running Contract Pool")
            for contract in self.contract_pool:
                if contract[0] == "chart":
                    await self.stock_chart(contract[2], contract[3], contract[4], contract[5], contract[6], contract[7], contract[8])
                    self.is_paused = False



async def add_contract_pool(self, ctx, contract):
    if contract:
        a = 1

async def add_limit_order(self, ctx, symbol, price, quantity):
    user_id = PBot
    order_type = "sell"
    cursor = self.conn.cursor()
    cursor.execute("""
        INSERT INTO limit_orders (user_id, symbol, order_type, price, quantity)
        VALUES (?, ?, ?, ?, ?)
    """, (user_id, symbol, order_type, price, quantity))
    order_id = cursor.lastrowid
    self.conn.commit()



async def unlock_share_limit(self, ctx, symbol, percentage, type):
    user_id = ctx.author.id

    if user_id in self.ceo_stocks:
        user_stocks = self.ceo_stocks[user_id]

        if symbol.lower() in user_stocks:
            total_supply, current_supply = await get_supply_info(self, ctx, symbol)
            amount = (Decimal(percentage) / 100) * Decimal(total_supply)
            await self.unlock_shares(ctx, symbol, int(amount), type)


async def get_total_shares_user_order(self, user_id, symbol):
    try:
        cursor = self.conn.cursor()

        # Get the total amount of shares for the specified user and stock
        cursor.execute("""
            SELECT COALESCE(SUM(quantity), 0) as total_shares
            FROM limit_orders
            WHERE user_id = ? AND symbol = ?
        """, (user_id, symbol))
        result = cursor.fetchone()

        return result["total_shares"]

    except sqlite3.Error as e:
        print(f"An error occurred while getting total shares: {e}")
        return 0  # Return 0 in case of an error

async def add_limit_orders(self, ctx, orders, verbose: bool = True):
    P3addrConn = sqlite3.connect("P3addr.db")
    PBotAddr = get_p3_address(P3addrConn, PBot)

    order_length = len(orders)
    embed = discord.Embed(description="New Limit Order")
    embed.add_field(name="Creating Limit Order", value=f"Orders: {order_length:,.0f}", inline=False)

    await ctx.send(embed=embed)
    order_id_list = []
    for order in orders:


        order_self = order[0]
        order_ctx = order[1]
        order_user_id = order[2]
        order_symbol = order[3]
        order_order_type = order[4]
        order_price = order[5]
        order_chunk_quantity = order[6]
        print(f"""
            {order[0]}
            {order[1]}
            {order[2]}
            {order[3]}
            {order[4]}
            {order[5]:,.0f}
            {order[6]:,.0f}
            {(order_length):,.0f}


        """)
        order_length -= 1
        if verbose:
            if order_order_type.lower() == "sell" and order_user_id != PBot:
                result = await send_stock(self, ctx, self.bot_address, get_p3_address(P3addrConn, order_user_id), order_symbol, order_chunk_quantity, False)
                if not result:
                    return
            else:
                tax_percentage = self.calculate_tax_percentage(ctx, "buy_stock")
                cost = order_price * order_chunk_quantity
                fee = Decimal(cost) * Decimal(tax_percentage)
                total = int(cost) + int(fee)
                await self.give_addr(ctx, PBotAddr, int(total), False)

        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO limit_orders (user_id, symbol, order_type, price, quantity)
            VALUES (?, ?, ?, ?, ?)
        """, (order_user_id, order_symbol, order_order_type, order_price, order_chunk_quantity))
        order_id_list.append(cursor.lastrowid)

        self.conn.commit()

    if verbose:
        embed = discord.Embed(description="Limit order added successfully.")
        embed.add_field(name="Order IDs:", value=f"{order_id_list[0]:,.0f}-{order_id_list[(len(order_id_list) - 1)]:,.0f}", inline=False)
        embed.add_field(name="Address:", value=f"{get_p3_address(P3addrConn, order_user_id)}", inline=False)
        embed.add_field(name="Stock:", value=f"{order_symbol}", inline=False)
        embed.add_field(name="Quantity:", value=f"{order_chunk_quantity:,.0f} per order", inline=False)
        embed.add_field(name="Order Type:", value=f"{order_order_type}", inline=False)
        embed.add_field(name="Price:", value=f"{order_price:,.2f}", inline=False)
        embed.add_field(name="Value:", value=f"{(order_price * order_chunk_quantity):,.2f}", inline=False)
        await ctx.send(embed=embed)



async def add_limit_orders2(self, ctx, orders, verbose: bool = True):
    P3addrConn = sqlite3.connect("P3addr.db")
    PBotAddr = get_p3_address(P3addrConn, PBot)

    order_length = len(orders)
    embed = discord.Embed(description="New Limit Order")
    embed.add_field(name="Creating Limit Order", value=f"Orders: {order_length:,.0f}", inline=False)

    await ctx.send(embed=embed)

    order_id_list = []

    # Reuse a single database connection
    cursor = self.conn.cursor()

    for order in orders:
        (
            order_self, order_ctx, order_user_id, order_symbol,
            order_order_type, order_price, order_chunk_quantity
        ) = order

        print(f"""
            {order_self}
            {order_ctx}
            {order_user_id}
            {order_symbol}
            {order_order_type}
            {order_price:,.0f}
            {order_chunk_quantity:,.0f}
            {(order_length):,.0f}
        """)

        order_length -= 1

        if verbose:
            if order_order_type.lower() == "sell" and order_user_id != PBot:
                result = await send_stock(self, ctx, self.bot_address, get_p3_address(P3addrConn, order_user_id), order_symbol, order_chunk_quantity, False)
                if not result:
                    return
            else:
                tax_percentage = self.calculate_tax_percentage(ctx, "buy_stock")
                cost = order_price * order_chunk_quantity
                fee = Decimal(cost) * Decimal(tax_percentage)
                total = int(cost) + int(fee)
                await self.give_addr(ctx, PBotAddr, int(total), False)

        # Append the values to the list
        order_id_list.append(cursor.lastrowid)

    # Execute a bulk insertion
    cursor.executemany("""
        INSERT INTO limit_orders (user_id, symbol, order_type, price, quantity)
        VALUES (?, ?, ?, ?, ?)
    """, [(order[2], order[3], order[4], order[5], order[6]) for order in orders])

    # Commit the changes
    self.conn.commit()

    if verbose:
        embed = discord.Embed(description="Limit order added successfully.")
        embed.add_field(name="Order IDs:", value=f"{order_id_list[0]:,.0f}-{order_id_list[-1]:,.0f}", inline=False)
        embed.add_field(name="Address:", value=f"{get_p3_address(P3addrConn, order_user_id)}", inline=False)
        embed.add_field(name="Stock:", value=f"{order_symbol}", inline=False)
        embed.add_field(name="Quantity:", value=f"{order_chunk_quantity:,.0f} per order", inline=False)
        embed.add_field(name="Order Type:", value=f"{order_order_type}", inline=False)
        embed.add_field(name="Price:", value=f"{order_price:,.2f}", inline=False)
        embed.add_field(name="Value:", value=f"{(order_price * order_chunk_quantity):,.2f}", inline=False)
        await ctx.send(embed=embed)



async def add_mv_metric(self, ctx, mv):
    cursor = self.conn.cursor()
    cursor.execute("""
        INSERT INTO market_value (mv)
        VALUES (?)
    """, (mv,))
    self.conn.commit()


async def add_reserve_metric(self, ctx):
    cursor = self.conn.cursor()

    # Calculate total stock value
    cursor.execute("SELECT balance FROM users WHERE user_id = ?", (PBot,))
    row = cursor.fetchone()
    qse = row[0]
    # Calculate total stock value
    cursor.execute("SELECT symbol, amount FROM user_stocks WHERE user_id=?", (PBot,))
    user_stocks = cursor.fetchall()
    stocks = 0

    for symbol, amount in user_stocks:
        cursor.execute("SELECT price FROM stocks WHERE symbol=?", (symbol,))
        stock_price_row = cursor.fetchone()
        stock_price = stock_price_row[0] if stock_price_row else 0
        stocks += stock_price * amount


    total = qse + stocks

    cursor.execute("""
        INSERT INTO reserve_value (qse, stocks, total)
        VALUES (?, ?, ?)
    """, (qse, stocks, total,))
    self.conn.commit()


async def calculate_average_mv(self):
    try:
        cursor = self.conn.cursor()
        cursor.execute("SELECT SUM(mv), COUNT(mv) FROM market_value")
        result = cursor.fetchone()

        if not result or result[1] == 0:
            return None  # Return None for an empty table or zero count

        total_mv, count = result
        average = total_mv / count
        return average
    except sqlite3.Error as e:
        print(f"An error occurred while calculating the average: {str(e)}")
        return None


async def remove_zero_quantity_orders_from_db(self, ctx, symbol):
    cursor = self.conn.cursor()

    # Fetch orders with quantity 0 for the specified symbol
    cursor.execute("SELECT * FROM limit_orders WHERE symbol=? AND quantity=0", (symbol,))
    zero_quantity_orders = cursor.fetchall()

    for order in zero_quantity_orders:
        order_id = order['order_id']
        await self.remove_limit_order_command(ctx, order_id)

    # Optionally, you can fetch the updated list after removal
    updated_orders = await fetch_sell_orders(self, symbol)
    return updated_orders


async def remove_limit_order(self, ctx, order_id):
    try:
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM limit_orders WHERE order_id=?", (order_id,))
        self.conn.commit()
    except sqlite3.Error as e:
        await ctx.send(f"An error occurred while removing a limit order: {e}")

async def remove_limit_orders(self, ctx, order_ids):
    if not order_ids:
        return  # No order IDs to remove

    try:
        cursor = self.conn.cursor()
        placeholders = ', '.join(['?'] * len(order_ids))
        cursor.execute(f"DELETE FROM limit_orders WHERE order_id IN ({placeholders})", order_ids)
        self.conn.commit()
    except sqlite3.Error as e:
        await ctx.send(f"An error occurred while removing limit orders: {e}")


def add_quantity_to_limit_order(self, order_id, amount):
    try:
        cursor = self.conn.cursor()

        # Check if the order_id exists
        cursor.execute("SELECT * FROM limit_orders WHERE order_id=?", (order_id,))
        existing_order = cursor.fetchone()

        if existing_order:
            current_quantity = existing_order['quantity']
            new_quantity = current_quantity + amount

            # Update the quantity for the specified order_id
            cursor.execute("UPDATE limit_orders SET quantity=? WHERE order_id=?", (new_quantity, order_id))
            self.conn.commit()

            print(f"Added {amount} to quantity of order_id {order_id}. New quantity: {new_quantity}")
        else:
            print(f"No order found with order_id {order_id}")

    except sqlite3.Error as e:
        print(f"An error occurred while adding quantity to limit order: {e}")

def remove_quantity_from_limit_order(self, order_id, amount):
    try:
        cursor = self.conn.cursor()

        # Check if the order_id exists
        cursor.execute("SELECT * FROM limit_orders WHERE order_id=?", (order_id,))
        existing_order = cursor.fetchone()

        if existing_order:
            current_quantity = existing_order['quantity']

            if current_quantity >= amount:
                new_quantity = current_quantity - amount

                # Update the quantity for the specified order_id
                cursor.execute("UPDATE limit_orders SET quantity=? WHERE order_id=?", (new_quantity, order_id))
                self.conn.commit()

                print(f"Removed {amount} from quantity of order_id {order_id}. New quantity: {new_quantity}")
            else:
                print(f"Cannot remove {amount} from order_id {order_id}. Insufficient quantity.")

        else:
            print(f"No order found with order_id {order_id}")

    except sqlite3.Error as e:
        print(f"An error occurred while removing quantity from limit order: {e}")


async def update_limit_order(self, ctx, order_id, new_quantity):
    try:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM limit_orders
            WHERE order_id = ?
        """, (order_id,))

        existing_order = cursor.fetchone()

        if existing_order:
            # Calculate the remaining quantity
            remaining_quantity = max(existing_order['quantity'] - new_quantity, 0)

            # Update the order with the new quantity
            cursor.execute("""
                UPDATE limit_orders
                SET quantity = ?
                WHERE order_id = ?
            """, (remaining_quantity, order_id))

            self.conn.commit()

#            embed = discord.Embed(description="Limit order updated successfully.")
#            embed.add_field(name="Order ID:", value=f"{order_id}", inline=False)
#            embed.add_field(name="Remaining Quantity:", value=f"{remaining_quantity:,.2f}", inline=False)
#            await ctx.send(embed=embed)
        else:
            #await ctx.send(f"No limit order found with order_id {order_id}.")
            pass
    except sqlite3.Error as e:
        await ctx.send(f"An error occurred while updating the limit order: {e}")


async def read_limit_orders(self, ctx):
    try:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM limit_orders")
        rows = cursor.fetchall()

        if not rows:
            embed = discord.Embed(description="No limit orders found.")
            await ctx.send(embed=embed)
        else:
            for row in rows:
                order_info = {
                    'order_id': row['order_id'],
                    'user_id': row['user_id'],
                    'symbol': row['symbol'],
                    'order_type': row['order_type'],
                    'price': row['price'],
                    'quantity': row['quantity'],
                    'created_at': row['created_at']
                }

                print(order_info)
                embed = discord.Embed(title="Limit Order Information", description=str(order_info))
                await ctx.send(embed=embed)
    except sqlite3.Error as e:
        embed = discord.Embed(description=f"An error occurred while reading limit orders: {e}")
        await ctx.send(embed=embed)

async def orders_at_price(self, ctx, order_type, symbol):
    try:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM limit_orders
            WHERE order_type = ? AND symbol = ?
            ORDER BY price ASC, created_at ASC
        """, (order_type, symbol))

        rows = cursor.fetchall()

        if rows:
            orders_info = [{
                'order_id': row['order_id'],
                'user_id': row['user_id'],
                'symbol': row['symbol'],
                'order_type': row['order_type'],
                'price': row['price'],
                'quantity': row['quantity'],
                'created_at': row['created_at']
            } for row in rows]

            return orders_info
        else:
            return []
    except sqlite3.Error as e:
        print(f"An error occurred while fetching orders at a specific price: {e}")
        return []

async def lowest_price_order(self, ctx, order_type, symbol, skip_user_id: bool = False):
    try:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM limit_orders
            WHERE order_type = ? AND symbol = ?
            ORDER BY price ASC
            LIMIT 1
        """, (order_type, symbol))

        row = cursor.fetchone()

        if row:
            order_info = {
                'order_id': row['order_id'],
                'user_id': row['user_id'],
                'symbol': row['symbol'],
                'order_type': row['order_type'],
                'price': row['price'],
                'quantity': row['quantity'],
                'created_at': row['created_at']
            }

            return order_info
        else:
            return False
    except sqlite3.Error as e:
        print(f"An error occurred while fetching the lowest price order: {e}")



async def highest_price_order(self, ctx, order_type, symbol):
    try:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM limit_orders
            WHERE order_type = ? AND symbol = ?
            ORDER BY price DESC
            LIMIT 1
        """, (order_type, symbol))

        row = cursor.fetchone()

        if row:
            order_info = {
                'order_id': row['order_id'],
                'user_id': row['user_id'],
                'symbol': row['symbol'],
                'order_type': row['order_type'],
                'price': row['price'],
                'quantity': row['quantity'],
                'created_at': row['created_at']
            }

            return order_info
        else:
            return False
    except sqlite3.Error as e:
        print(f"An error occurred while fetching the lowest price order: {e}")




async def calculate_min_price(self, ctx, stock_name: str):
    try:
        async with aiosqlite.connect("currency_system.db") as conn:
            await wait_for_unlocked(conn)
            cursor = await conn.cursor()

            # Fetch stock information
            await cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
            stock = await cursor.fetchone()

            if stock is None:
                return 0  # or handle the case where the stock doesn't exist

            available_supply = Decimal(stock[3])

            # Fetch circulating supply
            await cursor.execute("SELECT SUM(amount) FROM user_stocks WHERE symbol = ?", (stock_name,))
            circulating_supply = await cursor.fetchone()
            circulating_supply = Decimal(circulating_supply[0]) if circulating_supply else 0

            current_price = await get_stock_price(self, ctx, stock_name)

            # You can adjust this formula based on your specific requirements
            circulating_supply = float(circulating_supply)
            available_supply = float(available_supply)
            current_price = float(current_price)

            # Calculate the minimum price using your formula
            min_price = baseMinPrice + (circulating_supply / available_supply) * current_price

            # Ensure that the minimum price is strictly less than or equal to the current price
            min_price = max(min_price, 0)

            return min_price

    except aiosqlite.Error as e:
        print(f"An error occurred: {str(e)}")

async def calculate_max_price(self, ctx, stock_symbol):
    try:
        async with aiosqlite.connect("currency_system.db") as conn:
            await wait_for_unlocked(conn)
            cursor = await conn.cursor()

            # Get the current price of the stock
            await cursor.execute("SELECT price FROM stocks WHERE symbol=?", (stock_symbol,))
            current_price = await get_stock_price(self, ctx, stock_symbol)  # Convert to Decimal

            max_price_adjustment_factor = Decimal("0.1") * Decimal(current_price)

            # Fetch stock information
            await cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_symbol,))
            stock = await cursor.fetchone()

            if stock is None:
                return current_price  # or handle the case where the stock doesn't exist

            available_supply = Decimal(stock[3])

            # Fetch circulating supply
            await cursor.execute("SELECT SUM(amount) FROM user_stocks WHERE symbol = ?", (stock_symbol,))
            circulating_supply = await cursor.fetchone()
            circulating_supply = Decimal(circulating_supply[0]) if circulating_supply else 0

            # Calculate the daily volume for the stock
            total_buy_volume, total_sell_volume, total_volume = await calculate_volume(stock_symbol, interval='daily')

            if total_sell_volume is None:
                total_sell_volume = 1e-10
            if total_buy_volume is None:
                total_buy_volume = 1e-10

            # Calculate the ratio of total sell volume to total buy volume
            sell_buy_ratio = total_sell_volume / max(total_buy_volume, 1e-10)  # Adding a small value to avoid division by zero

            # Define the scaling factor function
            def calculate_scaling_factor(ratio):
                # You can customize this function based on how much you want total_sell_volume to affect the dynamic cap
                return 1 + (ratio * 0.5)

            # Calculate the scaling factor based on the sell-buy ratio
            scaling_factor = calculate_scaling_factor(sell_buy_ratio)

            min_cap = Decimal("0.0755")
            max_cap = 2.0
            current_price = Decimal(current_price)

            daily_volume = total_volume

            # Calculate the dynamic adjustment based on the daily volume
            volume_adjustment_factor = Decimal("0.0001") * Decimal(daily_volume)

            # Adjust the max_percentage_cap based on total_sell_volume
            dynamic_cap = min(max_cap, max(min_cap, max_cap - scaling_factor))

            # Calculate the new maximum price
            max_price = (current_price / circulating_supply) * max_price_adjustment_factor * volume_adjustment_factor

            # Ensure that the max_price is always higher than current_price
            max_price = max(max_price, current_price)

            # Ensure that max_price is not more than 25% over current_price when they are closer
            max_price = min(max_price, current_price * Decimal("1.357"))

            # Ensure that max_price is not equal to current_price
            if max_price == current_price:
                price_difference = Decimal(current_price) * Decimal("0.07523")  # Minimum of 10%

                # Introduce a random factor with a range of 0% to 15%
                random_factor = Decimal(str(random.uniform(0, 0.25532)))

                # Adjust the maximum change based on the current price
                max_change_percentage = min(Decimal(dynamic_cap), current_price / Decimal("100"))

                # Calculate the maximum change within the allowed percentage
                max_change = Decimal(random.uniform(0, float(max_change_percentage)))

                # Use the higher of the two random factors
                max_price_change = max(random_factor, max_change)

                max_price += max(price_difference, Decimal(current_price) * max_price_change)
                max_price = min(max_price, Decimal(20000000))  # Ensure it doesn't exceed 20,000,000


            return max_price

    except aiosqlite.Error as e:
        print(f"An error occurred: {str(e)}")



async def convert_to_float(value):
    # Helper function to convert a value to float, handling None and string conversion
    return float(value.replace(',', '')) if value is not None and isinstance(value, str) else 0.0

async def get_stock_price_interval(self, ctx, stock_symbol, interval='daily'):
    try:
        # Create a connection to the SQLite database using a context manager
        async with aiosqlite.connect("p3ledger.db") as conn:
            cursor = await conn.cursor()

            # Get the current date
            today = datetime.now().date()

            # Calculate the start and end timestamps based on the specified interval
            if interval == 'daily':
                start_timestamp = today
            elif interval == 'weekly':
                # Calculate the start of the week (Monday)
                start_timestamp = today - timedelta(days=today.weekday())
            elif interval == 'monthly':
                # Calculate the start of the month
                start_timestamp = today.replace(day=1)
            else:
                raise ValueError("Invalid interval. Supported intervals: 'daily', 'weekly', 'monthly'")

            # Fetch the opening price and the current price for the specified stock within the specified interval
            await cursor.execute("""
                SELECT
                    COALESCE((SELECT price FROM stock_transactions WHERE symbol=? AND timestamp <= ? AND action='Buy Stock' ORDER BY timestamp DESC LIMIT 1), 0.0),
                    COALESCE((SELECT price FROM stock_transactions WHERE symbol=? AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1), 0.0)
            """, (stock_symbol, start_timestamp, stock_symbol, today))

            opening_price, current_price = await cursor.fetchone()

            opening_price = await convert_to_float(opening_price)
            current_price = await get_stock_price(self, ctx, stock_symbol)
            current_price = await convert_to_float(current_price)

            price_change = current_price - opening_price

            return opening_price, current_price, price_change

    except aiosqlite.Error as e:
        print(f"An error occurred: {str(e)}")
        return 0, 0, 0


async def wait_for_unlocked(conn: aiosqlite.Connection):
    while True:
        try:
            # Try executing a simple query to check if the database is locked
            async with conn.execute("SELECT 1") as cursor:
                await cursor.fetchall()
            # If the query succeeds, the database is not locked
            break
        except aiosqlite.DatabaseLockedError:
            # Database is locked, wait for a short duration and try again
            await asyncio.sleep(0.1)
        except aiosqlite.OperationalError as e:
            # Some other operational error occurred, raise it
            raise e


async def count_all_transactions(interval='daily'):
    try:
        # Create a connection to the SQLite database
        conn = sqlite3.connect("p3ledger.db")
        cursor = conn.cursor()

        # Get the current date
        today = datetime.now().date()

        # Calculate the start and end timestamps based on the specified interval
        if interval == 'daily':
            start_timestamp = today.strftime("%Y-%m-%d 00:00:00")
        elif interval == 'weekly':
            # Calculate the start of the week (Monday)
            start_of_week = today - timedelta(days=today.weekday())
            start_timestamp = start_of_week.strftime("%Y-%m-%d 00:00:00")
        elif interval == 'monthly':
            # Calculate the start of the month
            start_of_month = today.replace(day=1)
            start_timestamp = start_of_month.strftime("%Y-%m-%d 00:00:00")
        else:
            raise ValueError("Invalid interval. Supported intervals: 'daily', 'weekly', 'monthly'")

        # Count the number of buy and sell transactions for all stocks within the specified interval
        cursor.execute("""
            SELECT
                COUNT(CASE WHEN action = 'Buy Stock' THEN 1 END) AS buy_count,
                COUNT(CASE WHEN action = 'Sell Stock' THEN 1 END) AS sell_count
            FROM stock_transactions
            WHERE timestamp >= ?
        """, (start_timestamp,))

        buy_count, sell_count = cursor.fetchone()

        return buy_count, sell_count

    except sqlite3.Error as e:
        print(f"An error occurred: {str(e)}")

    finally:
        # Close the database connection
        conn.close()


async def count_transactions(stock_name, interval='daily'):
    try:
        # Create a connection to the SQLite database
        conn = sqlite3.connect("p3ledger.db")
        cursor = conn.cursor()

        # Get the current date
        today = datetime.now().date()

        # Calculate the start and end timestamps based on the specified interval
        if interval == 'daily':
            start_timestamp = today.strftime("%Y-%m-%d 00:00:00")
            end_timestamp = today.strftime("%Y-%m-%d 23:59:59")
        elif interval == 'weekly':
            # Calculate the start of the week (Monday)
            start_of_week = today - timedelta(days=today.weekday())
            start_timestamp = start_of_week.strftime("%Y-%m-%d 00:00:00")
            end_timestamp = today.strftime("%Y-%m-%d 23:59:59")
        elif interval == 'monthly':
            # Calculate the start of the month
            start_of_month = today.replace(day=1)
            start_timestamp = start_of_month.strftime("%Y-%m-%d 00:00:00")
            end_timestamp = today.strftime("%Y-%m-%d 23:59:59")
        else:
            raise ValueError("Invalid interval. Supported intervals: 'daily', 'weekly', 'monthly'")

        # Count buy transactions
        cursor.execute("""
            SELECT COUNT(*)
            FROM stock_transactions
            WHERE symbol=? AND timestamp BETWEEN ? AND ? AND action='Buy Stock'
        """, (stock_name, start_timestamp, end_timestamp))
        buy_count = cursor.fetchone()[0]

        # Count sell transactions
        cursor.execute("""
            SELECT COUNT(*)
            FROM stock_transactions
            WHERE symbol=? AND timestamp BETWEEN ? AND ? AND action='Sell Stock'
        """, (stock_name, start_timestamp, end_timestamp))
        sell_count = cursor.fetchone()[0]

        return buy_count, sell_count

    except sqlite3.Error as e:
        print(f"An error occurred: {str(e)}")

    finally:
        # Close the database connection
        conn.close()
async def calculate_volume(stock_symbol, interval='daily'):
    try:
        async with aiosqlite.connect("p3ledger.db") as conn:
            await wait_for_unlocked(conn)
            cursor = await conn.cursor()

            # Get the current date and time
            now = datetime.now()

            # Calculate the start timestamp based on the specified interval
            if interval == 'daily':
                start_timestamp = now - timedelta(days=1)
            elif interval == 'weekly':
                start_timestamp = now - timedelta(days=now.weekday(), hours=now.hour, minutes=now.minute, seconds=now.second)
            elif interval == 'monthly':
                start_timestamp = datetime(now.year, now.month, 1)
            else:
                raise ValueError("Invalid interval. Supported intervals: 'daily', 'weekly', 'monthly'")

            # Fetch total buy, total sell, and total volume for the specified stock within the specified interval
            await cursor.execute("""
                SELECT
                    COALESCE(SUM(CASE WHEN action='Buy Stock' THEN quantity ELSE 0 END), 0.00000000000001) AS total_buy_volume,
                    COALESCE(SUM(CASE WHEN action='Sell Stock' THEN quantity ELSE 0 END), 0.00000000000001) AS total_sell_volume,
                    COALESCE(SUM(quantity), 0.00000000000001) AS total_volume
                FROM stock_transactions
                WHERE symbol=? AND (action='Buy Stock' OR action='Sell Stock') AND timestamp BETWEEN ? AND ?
            """, (stock_symbol, start_timestamp, now))

            total_buy_volume, total_sell_volume, total_volume = await cursor.fetchone()

            return total_buy_volume, total_sell_volume, total_volume

    except aiosqlite.Error as e:
        print(f"An error occurred: {str(e)}")





async def update_stock_price(self, ctx, stock_name: str, amount: float, buy: bool, verbose: bool = True, burn: bool = False):
    cursor = self.conn.cursor()
    cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
    stock = cursor.fetchone()

    if stock is None:
        await ctx.send(f"This stock does not exist.")
        return


    # Determine whether it's a buy or sell
    is_buy = buy


    if is_buy:
        # Buy logic
        avg_buy, avg_sell = await calculate_average_prices_by_symbol(self, stock_name)
        new_price = (avg_buy + avg_sell) / 2

    else:
        # Sell logic

        avg_buy, avg_sell = await calculate_average_prices_by_symbol(self, stock_name)
        new_price = (avg_buy + avg_sell) / 2

    cursor.execute("""
        UPDATE stocks
        SET price = ?
        WHERE symbol = ?
    """, (new_price, stock_name))


    self.conn.commit()





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


async def update_user_balance(conn, user_id, new_balance):
    try:
        if not isinstance(user_id, int) or new_balance is None:
            raise ValueError(f"Invalid user_id or new_balance value. user_id: {user_id}, new_balance: {new_balance:,.2f}")
        cursor = conn.cursor()
        # Ensure that new_balance is a Decimal
        new_balance = Decimal(new_balance)


        # Use a single SQL statement for both table creation and insertion
        cursor.execute("""
            INSERT OR REPLACE INTO users (user_id, balance)
            VALUES (?, ?)
        """, (user_id, new_balance))

        conn.commit()
    except ValueError as e:
        print(f"Error in update_user_balance: {e}")
        raise e

def decay_other_stocks(conn, stock_name_excluded, decay_percentage=stockDecayValue, decay_min=decayMin):
    """
    Reduces the value of all stocks other than the one specified by the given percentage.

    :param conn: Database connection
    :param stock_name_excluded: Stock symbol that should be excluded from the decay
    :param decay_percentage: The percentage by which the stocks should be decayed
    :param decay_min: The minimum value for the decayed price
    """
    cursor = conn.cursor()

    # Update all stocks with decay, excluding the specified stock
    cursor.execute("""
        UPDATE stocks
        SET price = CASE
            WHEN price * (1 - ?) > ? THEN price * (1 - ?)
            ELSE ?
        END
        WHERE symbol != ?
    """, (decay_percentage, decay_min, decay_percentage, decay_min, stock_name_excluded))

    conn.commit()



def update_user_stocks(user_id, stocks_reward_symbol, stocks_reward_amount, transaction_type):
    try:
        # Using context manager for database connection
        with sqlite3.connect("currency_system.db") as conn:
            cursor = conn.cursor()

            if transaction_type.lower() not in {"buy", "sell"}:
                raise ValueError("Invalid transaction type. Please use 'buy' or 'sell'.")

            # Use a single SQL statement with a parameterized query to handle both buy and sell cases
            cursor.execute(f"""
                UPDATE stocks
                SET available = available {'-' if transaction_type.lower() == 'buy' else '+'} ?
                WHERE symbol = ?
            """, (stocks_reward_amount, stocks_reward_symbol))

            # Insert or update user_stocks table
            cursor.execute("""
                INSERT INTO user_stocks (user_id, symbol, amount)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id, symbol)
                DO UPDATE SET amount = amount + ?""",
                (user_id, stocks_reward_symbol, stocks_reward_amount, stocks_reward_amount))

            # Commit the changes to the database
            conn.commit()

    except sqlite3.Error as e:
        print(f"Error updating user stocks: {e}")

    except ValueError as ve:
        print(f"ValueError: {ve}")

    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")

async def update_available_stock(ctx, stock_name, amount, transaction_type):
    try:
        # Using context manager for database connection
        with sqlite3.connect("currency_system.db") as conn:
            cursor = conn.cursor()

            conn.execute("BEGIN")

            if transaction_type.lower() == "buy":
                # Check if available stock will go below zero
                cursor.execute("SELECT available FROM stocks WHERE symbol=?", (stock_name,))
                available_stock = cursor.fetchone()[0]
                if available_stock < amount:
                    await ctx.send(f"{ctx.author.mention}, insufficient available stock for {stock_name}.")
                    return

                cursor.execute("""
                    UPDATE stocks
                    SET available = available - ?
                    WHERE symbol = ?
                """, (amount, stock_name))
            elif transaction_type.lower() == "sell":
                cursor.execute("""
                    UPDATE stocks
                    SET available = available + ?
                    WHERE symbol = ?
                """, (amount, stock_name))
            else:
                await ctx.send(f"{ctx.author.mention}, invalid transaction type. Please use 'buy' or 'sell'.")
                return

            conn.commit()

    except sqlite3.Error as e:
        await ctx.send(f"{ctx.author.mention}, an error occurred while updating available stocks. Error: {str(e)}")
        return



async def check_store_addr(self, ctx):
    target_address = get_p3_address(self.P3addrConn, ctx.author.id)
    # Get user_id associated with the target address
    user_id = get_user_id(self.P3addrConn, target_address)

    if not user_id:
        await ctx.send("P3 Address not found, type !store_addr to get one.")
        return user_id

    else:
        return target_address


def calculate_interest(self, amount, interest_rate, elapsed_days):
    # Assuming simple interest for the sake of simplicity
    weekly_interest = (interest_rate / 100) * amount
    total_interest = weekly_interest * (elapsed_days // 7)
    return total_interest

def get_interest_rate(self, lock_duration):
    # Map lock durations to corresponding interest rates
    interest_rates = {30: 10, 60: 13, 90: 16, 120: 22}
    return interest_rates.get(lock_duration, 0)

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
        max_tax_rate = 0.25 / (1 + math.exp(float(cost) / -500000000))

    # Formulaic approach (based on logarithmic progression)
    quantity_multiplier = 0.001 * (quantity ** 0.5)
    cost_multiplier = 0.001 * (float(cost) / 1000) ** 0.5

    tax_rate = base_tax + quantity_multiplier + cost_multiplier + random_factor + seasonal_discount

    # Limit the tax rate to the maximum tax on Mondays
    tax_rate = min(tax_rate, max_tax_rate)

    # Clipping the tax_rate between 0.01 (1%) to 0.5 (50%) to ensure it's not too low or too high
    tax_rate = max(0.01, min(tax_rate, 0.5))

    return tax_rate


async def get_supply_stats(self, ctx, symbol):
    reserve = self.get_user_stock_amount(PBot, symbol)
    total, locked = await get_supply_info(self, ctx, symbol)
    escrow = await get_total_shares_in_orders(self, symbol)
    market = reserve - escrow
    circulating = total - escrow - locked

    return reserve, total, locked, escrow, market, circulating





async def get_supply_info(self, ctx, stock_name):
    cursor = self.conn.cursor()

    # Retrieve current supply amounts
    cursor.execute("SELECT total_supply, available FROM stocks WHERE symbol=?", (stock_name,))
    supply_info = cursor.fetchone()

    if supply_info is None:
        await ctx.send(f"This stock does not exist.")
        return

    return supply_info


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

    # Use JOIN to fetch all relevant data in a single query
    cursor.execute("""
        SELECT s.price, es.quantity
        FROM stocks s
        JOIN etf_stocks es ON s.symbol = es.symbol
        WHERE es.etf_id = ?
    """, (etf_id,))

    stocks_data = cursor.fetchall()

    # Calculate the value of the ETF
    etf_value = sum(price * quantity for price, quantity in stocks_data)

    return etf_value


async def get_etf_stocks(conn, etf_id):
    cursor = conn.cursor()
    cursor.execute("SELECT symbol FROM etf_stocks WHERE etf_id=?", (etf_id,))

    # Fetch all rows and extract symbols
    symbols = [row["symbol"] for row in cursor.fetchall()]

    return symbols

async def get_total_shares_in_orders(self, symbol):
    try:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT SUM(quantity) as total_shares
            FROM limit_orders
            WHERE symbol = ? AND (order_type = 'sell' OR order_type = 'buy')
        """, (symbol,))

        result = cursor.fetchone()

        return result['total_shares'] if result and result['total_shares'] else 0

    except sqlite3.Error as e:
        print(f"An error occurred while fetching total shares in orders: {e}")
        return 0


async def get_total_shares_in_escrow_all_stocks(self):
    try:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT SUM(quantity) as total_shares
            FROM limit_orders
            WHERE order_type IN ('sell', 'buy')
        """)

        result = cursor.fetchone()

        if result and result['total_shares']:
            return result['total_shares']
        else:
            return 0
    except sqlite3.Error as e:
        print(f"An error occurred while fetching total shares in escrow for all stocks: {e}")
        return 0

async def get_total_shares_in_escrow_user(self, user_id):
    try:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT SUM(quantity) as total_shares
            FROM limit_orders
            WHERE user_id = ? AND (order_type = 'sell' OR order_type = 'buy')
        """, (user_id,))

        result = cursor.fetchone()

        if result and result['total_shares']:
            return result['total_shares']
        else:
            return 0
    except sqlite3.Error as e:
        print(f"An error occurred while fetching total shares in escrow for user {user_id}: {e}")
        return 0



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

async def log_transaction(ledger_conn, ctx, action, symbol, quantity, pre_tax_amount, post_tax_amount, balance_before, balance_after, price, verbose):
    # Get the user's username from the context
    if balance_before == 0 and balance_after == 0:
        P3Addr = "P3:03da907038"
    else:
        username = ctx.author.name
        P3Addr = generate_crypto_address(ctx.author.id)

    # Batch convert Decimal values to strings
    values_to_string = lambda *values: [str(value) for value in values]
    pre_tax_amount_str, post_tax_amount_str, balance_before_str, balance_after_str, price_str = values_to_string(
        pre_tax_amount, post_tax_amount, balance_before, balance_after, price
    )

    # Convert Decimal values to strings
    pre_tax_amount_str = str(pre_tax_amount)
    post_tax_amount_str = str(post_tax_amount)
    balance_before_str = str(balance_before)
    balance_after_str = str(balance_after)
    price_str = '{:,.11f}'.format(price) if symbol.lower() == "roflstocks" else '{:,.2f}'.format(price)

    # Create a timestamp for the transaction
    timestamp = ctx.message.created_at.strftime("%Y-%m-%d %H:%M:%S")

    # Insert the transaction into the stock_transactions table
    cursor = ledger_conn.cursor()
    cursor.execute("""
        INSERT INTO stock_transactions (user_id, action, symbol, quantity, pre_tax_amount, post_tax_amount, balance_before, balance_after, price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (ctx.author.id, action, symbol, quantity, pre_tax_amount_str, post_tax_amount_str, balance_before_str, balance_after_str, price_str))
    ledger_conn.commit()

    if verbose == "True":
        # Determine whether it's a stock or ETF transaction
        is_etf = True if action in ["Buy ETF", "Sell ETF"] else False

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
                    description=f"Quantity: {quantity:,.0f}\n"
                                f"Price: {price:,.2f} $QSE\n"
                                f"Pre-Gas Amount: {pre_tax_amount:,.2f} $QSE\n"
                                f"Post-Gas Amount: {post_tax_amount:,.2f} $QSE\n"
                                f"Balance Before: {balance_before:,.2f} $QSE\n"
                                f"Balance After: {balance_after:,.2f} $QSE\n"
                                f"Timestamp: {timestamp}",
                    color=discord.Color.green() if action.startswith("Buy") else discord.Color.red()
                )

                # Send the log message as an embed to the specified channels
                await channel1.send(embed=embed)



async def mint_to_reserve(self, ctx, amount: int):
    current_balance = get_user_balance(self.conn, PBot)
    new_balance = current_balance + amount
    await update_user_balance(self.conn, PBot, new_balance)

async def log_order_transaction(ledger_conn, ctx, action, symbol, quantity, pre_tax_amount, post_tax_amount, balance_before, balance_after, price, verbose, userid):
    # Get the user's username from the context
    if balance_before == 0 and balance_after == 0:
        P3Addr = "P3:03da907038"
    else:
#        username = ctx.author.name
        P3Addr = generate_crypto_address(userid)

    # Batch convert Decimal values to strings
    values_to_string = lambda *values: [str(value) for value in values]
    pre_tax_amount_str, post_tax_amount_str, balance_before_str, balance_after_str, price_str = values_to_string(
        pre_tax_amount, post_tax_amount, balance_before, balance_after, price
    )

    # Convert Decimal values to strings
    pre_tax_amount_str = str(pre_tax_amount)
    post_tax_amount_str = str(post_tax_amount)
    balance_before_str = str(balance_before)
    balance_after_str = str(balance_after)
    price_str = '{:,.11f}'.format(price) if symbol.lower() == "roflstocks" else '{:,.2f}'.format(price)

    # Create a timestamp for the transaction
    timestamp = ctx.message.created_at.strftime("%Y-%m-%d %H:%M:%S")

    # Insert the transaction into the stock_transactions table
    cursor = ledger_conn.cursor()
    cursor.execute("""
        INSERT INTO stock_transactions (user_id, action, symbol, quantity, pre_tax_amount, post_tax_amount, balance_before, balance_after, price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (userid, action, symbol, quantity, pre_tax_amount_str, post_tax_amount_str, balance_before_str, balance_after_str, price_str))
    ledger_conn.commit()

    if verbose == "True":
        # Determine whether it's a stock or ETF transaction
        is_etf = True if action in ["Buy ETF", "Sell ETF"] else False

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
                                f"Price: {price} $QSE\n"
                                f"Pre-Gas Amount: {pre_tax_amount:,.2f} $QSE\n"
                                f"Post-Gas Amount: {post_tax_amount:,.2f} $QSE\n"
                                f"Balance Before: {balance_before:,.2f} $QSE\n"
                                f"Balance After: {balance_after:,.2f} $QSE\n"
                                f"Timestamp: {timestamp}",
                    color=discord.Color.green() if action.startswith("Buy") else discord.Color.red()
                )

                # Send the log message as an embed to the specified channels
                await channel1.send(embed=embed)




async def log_transfer(self, ledger_conn, ctx, sender_name, receiver_name, receiver_id, amount, is_burn=False):
    self.transfer_timer_start = timeit.default_timer()
    guild_id = 1161678765894664323
    guild = ctx.bot.get_guild(guild_id)

    if receiver_id == PBot:
        pass
    else:

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
                    description=f"Amount: {amount:,.2f} $QSE",
                    color=discord.Color.purple()
                )

                if is_burn:
                    embed.title = f"Burn of {amount:,.2f} $QSE"
                    embed.color = discord.Color.red()
                    embed.description = f"{sender_name} burned {amount:,.2f} $QSE"
                    # Add any additional fields for burn logs if needed

#                await channel1.send(embed=embed)
        elapsed_time = timeit.default_timer() - self.transfer_timer_start
        self.transfer_avg.append(elapsed_time)



async def log_stock_transfer(self, ledger_conn, ctx, sender, receiver, symbol, amount):
    self.transfer_timer_start = timeit.default_timer()
    try:
        guild_id = 1161678765894664323
        guild = ctx.bot.get_guild(guild_id)

        if amount == 0.0:
            return

        if guild:
            P3Addr_sender = "P3:0x00000000" if sender == "stakingYield" else generate_crypto_address(sender.id)
            P3Addr_receiver = generate_crypto_address(receiver) if isinstance(receiver, int) else generate_crypto_address(receiver.id)

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


    elapsed_time = timeit.default_timer() - self.transfer_timer_start
    self.transfer_avg.append(elapsed_time)

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
                description=f"Item: {item_name}\nQuantity: {quantity:,.2f}\nTotal Cost: {total_cost:,.2f} $QSE\n"
                            f"Gas Amount: {tax_amount:,.2f} $QSE\nNew Balance: {new_balance:,.2f} $QSE\nTimestamp: {timestamp}",
                color=discord.Color.green() if is_purchase else discord.Color.red()
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
                description=f"Game: {game}\nBet Amount: {bet_amount} $QSE\nWin/Loss: {win_loss}\n"
                            f"Amount Paid/Received After Gas: {amount_after_tax:,.2f} $QSE",
                color=discord.Color.orange() if win_loss.startswith("You won") else discord.Color.orange()
            )

            await channel1.send(embed=embed)

async def record_user_daily_buy(cursor, user_id, stock_name, amount):
    try:
        cursor.execute("""
            INSERT INTO user_daily_buys (user_id, symbol, amount, timestamp)
            VALUES (?, ?, ?, datetime('now'))
        """, (user_id, stock_name, amount))
    except sqlite3.Error as e:
        print(f"Error recording daily stock buy: {e}")

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






# Begin level




def create_users_level_table(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users_level (
                user_id INTEGER PRIMARY KEY,
                level INTEGER NOT NULL,
                experience INTEGER NOT NULL
            )
        """)
        conn.commit()
        print("Table 'users_level' created successfully.")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the 'users_level' table: {e}")

def insert_user_level(conn, user_id, level, experience):
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO users_level (user_id, level, experience) VALUES (?, ?, ?)", (user_id, level, experience))
        conn.commit()
        print("User level inserted successfully.")
    except sqlite3.Error as e:
        print(f"Error inserting user level: {e}")

def update_user_level(conn, user_id, level, experience):
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE users_level SET level=?, experience=? WHERE user_id=?", (level, experience, user_id))
        conn.commit()
        print("User level updated successfully.")
    except sqlite3.Error as e:
        print(f"Error updating user level: {e}")

def get_user_level(conn, user_id):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT level, experience FROM users_level WHERE user_id=?", (user_id,))
        row = cursor.fetchone()

        if row is not None:
            level, experience = row
            print(f"User {user_id} - Level: {level}, Experience: {experience}")
            return level, experience
        else:
            print(f"User {user_id} not found. Setting default values.")
            # Set default values for level and experience
            default_level = 1
            default_experience = 0
            return default_level, default_experience
    except sqlite3.Error as e:
        print(f"Error getting user level: {e}")
        # Handle the error, you can return default values or raise an exception
        default_level = 1
        default_experience = 0
        return default_level, default_experience


def delete_user_level(conn, user_id):
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM users_level WHERE user_id=?", (user_id,))
        conn.commit()
        print("User level deleted successfully.")
    except sqlite3.Error as e:
        print(f"Error deleting user level: {e}")

def get_user_experience_info(conn, user_id):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT level, experience FROM users_level WHERE user_id=?", (user_id,))
        row = cursor.fetchone()

        if row:
            level, experience = row
            return level, experience
        else:
            # If user not found, return default values
            return 1, 0
    except sqlite3.Error as e:
        print(f"Error getting user level and experience: {e}")

async def add_experience(self, conn, user_id, points, ctx):
    try:
        cursor = conn.cursor()

        # Check if the user exists in the table
        cursor.execute("SELECT user_id, level, experience FROM users_level WHERE user_id=?", (user_id,))
        user_data = cursor.fetchone()

        if user_data:
            user_id, level, experience = user_data
            new_experience = experience + points

            # Check if the user levels up
            while new_experience >= (level * 100):
                new_experience -= level * 100
                level += 1

                # Use the updated level_up function to show the total change in stats
                await level_up(self, ctx, user_id, 1)

            # Update user's level and experience
            cursor.execute("UPDATE users_level SET level=?, experience=? WHERE user_id=?", (level, new_experience, user_id))
        else:
            # User doesn't exist, insert with default values
            cursor.execute("INSERT INTO users_level (user_id, level, experience) VALUES (?, 1, ?)", (user_id, points))

        conn.commit()

    except sqlite3.Error as e:
        print(f"Error adding experience points: {e}")

def calculate_experience_for_level(level):
    # Adjust this formula based on your desired progression
    return int(100 * (1.5 ** (level - 1)))


def generate_experience_table(levels):
    experience_table = {level: int(100 * (1.5 ** (level - 1))) for level in range(1, levels + 1)}
    return experience_table



# UpDown Options


async def send_dm_user(self, user_id, message):
    try:
        user = await self.bot.fetch_user(user_id)
        if user:
            await user.send(message)
            return True
        else:
            print(f"User with ID {user_id} not found.")
            return False
    except discord.Forbidden:
        print(f"Failed to send DM to {user_id}: Forbidden (user has DMs disabled)")
        return False


async def autoliquidate(self, ctx):
    updown_assets = ["BlueChipOG"]
    updown_assets.extend(self.etfs)

    try:
        orders_to_delete = []

        for check_asset in updown_assets:
            if check_asset in self.etfs:
                asset_price = await get_etf_value(self.conn, int(check_asset))
            else:
                asset_price = await get_stock_price(self, ctx, check_asset)

            below_limit_users, above_limit_users = check_price_against_limits(self, asset_price, check_asset)

            expired_contracts = get_expired_contracts(self)
            if expired_contracts:
                for _, _, _, _, _, _, order_id in expired_contracts:
                    orders_to_delete.append(order_id)

                expired_contracts.clear()

            if below_limit_users:
                for _, order_id in below_limit_users:
                    orders_to_delete.append(order_id)

                below_limit_users.clear()

            if above_limit_users:
                for _, order_id in above_limit_users:
                    orders_to_delete.append(order_id)

                above_limit_users.clear()

        # Delete orders in bulk
        await delete_orders(self, orders_to_delete)

    except Exception as e:
        print(f"An unexpected error occurred in autoliquidate: {e}")

async def delete_orders(self, order_ids):
    try:
        cursor = self.conn.cursor()

        # Check if order_ids is not empty
        if not order_ids:
            return True

        # Convert the order_ids list to a NumPy array
        order_ids_array = np.array(order_ids)

        # Chunk the order_ids into smaller portions (chunk size can be adjusted)
        chunk_size = 10000  # You can adjust this value based on your database's limit
        for chunk in np.array_split(order_ids_array, np.ceil(len(order_ids_array) / chunk_size)):
            print(f"{len(order_ids_array):,.0f}")
            placeholders = ','.join('?' for _ in chunk)
            query = f"DELETE FROM updown_orders WHERE order_id IN ({placeholders})"
            cursor.execute(query, tuple(chunk))

        self.conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"An error occurred while deleting orders: {e}")
        return False






def count_user_contracts(self, user_id, asset):
    try:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM updown_orders
            WHERE user_id = ? AND asset = ? AND expiration > CURRENT_TIMESTAMP
        """, (user_id, asset))
        result = cursor.fetchone()

        if result:
            return result[0]
        else:
            return 0
    except sqlite3.Error as e:
        print(f"An error occurred while counting user contracts: {e}")
        return 0

def get_total_current_prices(self):
    try:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT SUM(current_price)
            FROM updown_orders
            WHERE expiration > CURRENT_TIMESTAMP
        """)
        total_current_prices = cursor.fetchone()[0]

        return total_current_prices if total_current_prices else 0
    except sqlite3.Error as e:
        print(f"An error occurred while getting total current prices: {e}")
        return 0

def check_price_against_limits(self, price, asset):
    try:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT user_id, lower_limit, upper_limit, order_id
            FROM updown_orders
            WHERE asset = ? AND expiration > CURRENT_TIMESTAMP
        """, (asset,))
        results = cursor.fetchall()

        user_data_below_lower_limit = []
        user_data_above_upper_limit = []

        for result in results:
            user_id, lower_limit, upper_limit, order_id = result

            if price < lower_limit:
                user_data_below_lower_limit.append((user_id, order_id))
            elif price > upper_limit:
                user_data_above_upper_limit.append((user_id, order_id))

        return user_data_below_lower_limit, user_data_above_upper_limit
    except sqlite3.Error as e:
        print(f"An error occurred while checking price against limits: {e}")
        return [], []



def get_expired_contracts(self):
    try:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT user_id, asset, current_price, lower_limit, upper_limit, expiration, order_id
            FROM updown_orders
            WHERE expiration <= CURRENT_TIMESTAMP
        """)
        expired_contracts = cursor.fetchall()

        return expired_contracts
    except sqlite3.Error as e:
        print(f"An error occurred while getting expired contracts: {e}")
        return []

def close_all_contracts(self):
    try:
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE updown_orders
            SET expiration = CURRENT_TIMESTAMP
            WHERE expiration > CURRENT_TIMESTAMP
        """)
        self.conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"An error occurred while closing all contracts: {e}")
        return False

def close_all_contracts_user(self, user_id):
    try:
        cursor = self.conn.cursor()

        # Delete all rows where user_id matches and expiration is in the future
        cursor.execute("""
            DELETE FROM updown_orders
            WHERE user_id = ? AND expiration > CURRENT_TIMESTAMP
        """, (user_id,))

        # Commit the changes to the database
        self.conn.commit()

        # Indicate that the operation was successful
        return True

    except sqlite3.Error as e:
        # Print an error message if an exception occurs
        print(f"An error occurred while closing all contracts: {e}")

        # Indicate that the operation was not successful
        return False


async def add_updown_order(self, user_id, asset, current_price, lower_limit, upper_limit, contract_date, expiration):
    try:
        order_id = str(uuid.uuid4())  # Generate a UUID as the order_id
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO updown_orders (user_id, asset, current_price, lower_limit, upper_limit, contract_date, expiration, order_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (user_id, asset, current_price, lower_limit, upper_limit, contract_date, expiration, order_id))
        self.conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"An error occurred while adding an UpDown order: {e}")
        return False

async def add_multiple_updown_orders(self, orders):
    try:
        order_ids = [str(uuid.uuid4()) for _ in range(len(orders))]

        cursor = self.conn.cursor()

        # Using executemany with parameter substitution and Decimal arithmetic
        cursor.executemany("""
            INSERT INTO updown_orders (user_id, asset, current_price, lower_limit, upper_limit, contract_date, expiration, order_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [(order['user_id'], order['asset'], Decimal(order['current_price']), Decimal(order['lower_limit']),
               Decimal(order['upper_limit']), order['contract_date'], order['expiration'], order_id)
              for order, order_id in zip(orders, order_ids)])

        self.conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"An error occurred while adding multiple UpDown orders: {e}")
        # Add additional error handling or logging here
        return False


def get_current_price(self, user_id):
    try:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT current_price FROM updown_orders WHERE user_id = ?
        """, (user_id,))
        result = cursor.fetchone()
        return result[0] if result else None
    except sqlite3.Error as e:
        print(f"An error occurred while getting current price: {e}")

def check_expiration_status(self, user_id):
    try:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT expiration FROM updown_orders WHERE user_id = ?
        """, (user_id,))
        result = cursor.fetchone()
        if result:
            expiration_time = datetime.strptime(result[0], "%Y-%m-%d %H:%M:%S")
            current_time = datetime.now()
            return current_time > expiration_time
        else:
            return None
    except sqlite3.Error as e:
        print(f"An error occurred while checking expiration status: {e}")


async def get_user_options(self, user_id):
    try:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM updown_orders
            WHERE user_id = ? AND expiration > CURRENT_TIMESTAMP
        """, (user_id,))
        results = cursor.fetchall()

        user_options = []
        for result in results:
            user_option = {
                "asset": result[1],
                "current_price": result[2],
                "lower_limit": result[3],
                "upper_limit": result[4],
                "contract_date": result[5],
                "expiration": result[6],
            }
            user_options.append(user_option)

        return user_options
    except sqlite3.Error as e:
        print(f"An error occurred while getting user options: {e}")
        return []


async def calculate_average_prices_by_symbol(self, symbol):
    try:
        async with aiosqlite.connect("p3ledger.db") as ledger_conn:
            ledger_conn.row_factory = aiosqlite.Row

            async with ledger_conn.execute("""
                SELECT timestamp, action, price
                FROM stock_transactions
                WHERE symbol=? AND (action='Buy Stock' OR action='Sell Stock')
                ORDER BY timestamp
            """, (symbol,)) as result:
                transactions = await result.fetchall()

            if transactions:
                average_buy_price, average_sell_price = await calculate_average_prices(transactions, None)
                avg_price = (average_buy_price or 0) + (average_sell_price or 0)
                avg_price /= 2
                return average_buy_price, average_sell_price
            else:
                return 0, 0

    except aiosqlite.Error as e:
        print(f"Error accessing ledger database: {e}")
        return 0, 0




def calculate_option_range(self, current_price):
    lower_limit = current_price * (1 - self.UpDownPerRange)
    upper_limit = current_price * (1 + self.UpDownPerRange)
    return lower_limit, upper_limit

async def calculate_average_prices(transactions, time_limit=None):
    buy_prices = []
    sell_prices = []

    for timestamp_str, action, price_str in transactions:
        formatted_price = str(price_str).replace(",", "") if isinstance(price_str, str) else price_str
        price = float(formatted_price)
        datetime_obj = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")  # Parse timestamp string

        if action == 'Buy Stock':
            buy_prices.append((datetime_obj, price))
        elif action == 'Sell Stock':
            sell_prices.append((datetime_obj, price))

    # If there's a time limit, filter transactions accordingly
    if time_limit:
        buy_prices = [(timestamp, price) for timestamp, price in buy_prices if timestamp >= time_limit]
        sell_prices = [(timestamp, price) for timestamp, price in sell_prices if timestamp >= time_limit]

    # Calculate average buy and sell prices
    average_buy_price = np.mean([price for _, price in buy_prices]) if buy_prices else 0
    average_sell_price = np.mean([price for _, price in sell_prices]) if sell_prices else 0

    return average_buy_price, average_sell_price


## End UpDown

## Futures

def place_futures_order(self, user_id, symbol, order_type, price, quantity, expiration):
    try:
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO futures_orders (user_id, symbol, order_type, price, quantity, expiration)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (user_id, symbol, order_type, price, quantity, expiration))
        self.conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"Error placing futures order: {e}")
        return False

def check_futures_orders(self, current_price, symbol):
    try:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT user_id, order_type, price, quantity, expiration
            FROM futures_orders
            WHERE symbol = ? AND expiration > CURRENT_TIMESTAMP
        """, (symbol,))
        results = cursor.fetchall()

        liquidation_candidates = []

        for result in results:
            user_id, order_type, price, quantity, expiration = result

            if (order_type == 'buy' and price >= current_price) or (order_type == 'sell' and price <= current_price):
                liquidation_candidates.append({
                    'user_id': user_id,
                    'order_type': order_type,
                    'quantity': quantity,
                    'profit_loss': (current_price - price) * quantity
                })

        return liquidation_candidates
    except sqlite3.Error as e:
        print(f"Error checking futures orders: {e}")
        return []

def liquidate_futures_order(self, user_id, order_type, quantity, profit_loss):
    try:
        cursor = self.conn.cursor()
        cursor.execute("""
            DELETE FROM futures_orders
            WHERE user_id = ? AND order_type = ? AND quantity = ?
        """, (user_id, order_type, quantity))
        self.conn.commit()

        # Perform actions based on liquidation (e.g., update user balance)
        # ...

        return True
    except sqlite3.Error as e:
        print(f"Error liquidating futures order: {e}")
        return False


## End Derivatives
level_conn = sqlite3.connect("currency_system.db")

def create_user_rpg_stats(user_id):
    try:
        cursor = level_conn.cursor()
        cursor.execute("""
            INSERT INTO users_rpg_stats (user_id, cur_hp, max_hp, atk, def, eva, luck, chr, spd)
            VALUES (?, 10, 10, 1, 1, 1, 1, 1, 1)
        """, (user_id,))
        level_conn.commit()
    except sqlite3.Error as e:
        print(f"Error creating RPG stats for user {user_id}: {e}")

def update_rpg_stat(user_id, stat, value):
    try:
        cursor = level_conn.cursor()
        cursor.execute(f"UPDATE users_rpg_stats SET {stat} = ? WHERE user_id = ?", (value, user_id))
        level_conn.commit()
    except sqlite3.Error as e:
        print(f"Error updating {stat} for user {user_id}: {e}")



async def level_up(self, ctx, user_id, levels):
    # Specify the stats you want to update and their corresponding roll range
    stat_roll_ranges = {
        'cur_hp': (5, 20),
        'max_hp': (5, 20),
        'atk': (1, 5),
        'def': (1, 5),
        'eva': (1, 5),
        'luck': (1, 5),
        'chr': (1, 5),
        'spd': (1, 5),
    }
    p3addr = get_p3_address(self.P3addrConn, user_id)
    try:
        cursor = level_conn.cursor()

        # Create an embed to display the old stats
        old_stats_embed = discord.Embed(title=f"Old RPG Stats for User {p3addr}", color=discord.Colour.orange())

        for stat in stat_roll_ranges.keys():
            current_value = cursor.execute(f"SELECT {stat} FROM users_rpg_stats WHERE user_id=?", (user_id,)).fetchone()[0]
            old_stats_embed.add_field(name=stat, value=current_value, inline=True)

        # Send the embed with old stats
        await ctx.send(embed=old_stats_embed)

        total_change = {stat: 0 for stat in stat_roll_ranges}

        for _ in range(levels):
            for stat, roll_range in stat_roll_ranges.items():
                # Use the rolled value for both cur_hp and max_hp
                roll_value = random.randint(*roll_range) if stat not in ['cur_hp', 'max_hp'] else random.randint(*stat_roll_ranges['cur_hp'])

                # Update the total change for the stat
                total_change[stat] += roll_value

        # Apply the total change to the RPG stats
        for stat, change in total_change.items():
            current_value = cursor.execute(f"SELECT {stat} FROM users_rpg_stats WHERE user_id=?", (user_id,)).fetchone()[0]
            update_rpg_stat(user_id, stat, current_value + change)

        # Create an embed to display the total change in stats
        change_stats_embed = discord.Embed(title=f"Total Change in RPG Stats for User {p3addr}", color=discord.Colour.green())

        for stat, change in total_change.items():
            change_stats_embed.add_field(name=stat, value=f"+{change}", inline=True)

        # Send the embed with total change in stats
        await ctx.send(embed=change_stats_embed)

        # Create an embed to display the new stats
        new_stats_embed = discord.Embed(title=f"New RPG Stats for User {p3addr}", color=discord.Colour.green())

        for stat in stat_roll_ranges.keys():
            new_value = cursor.execute(f"SELECT {stat} FROM users_rpg_stats WHERE user_id=?", (user_id,)).fetchone()[0]
            new_stats_embed.add_field(name=stat, value=new_value, inline=True)

        # Send the embed with new stats
        await ctx.send(embed=new_stats_embed)

    except sqlite3.Error as e:
        print(f"Error updating RPG stats for user {user_id}: {e}")

def get_rpg_stats(user_id):
    try:
        cursor = level_conn.cursor()
        cursor.execute("SELECT * FROM users_rpg_stats WHERE user_id=?", (user_id,))
        result = cursor.fetchone()
        if result:
            return {'cur_hp': result[1], 'max_hp': result[2], 'atk': result[3], 'def': result[4], 'eva': result[5],
                    'luck': result[6], 'chr': result[7], 'spd': result[8]}
        else:
            return None

    except sqlite3.Error as e:
        print(f"Error retrieving RPG stats for user {user_id}: {e}")
        return None


def calculate_damage(attacker_stats, defender_stats):
    # Calculate base damage based on attacker's attack
    base_damage = random.randint(1, attacker_stats['atk'])

    # Adjust damage based on various factors
    attack_multiplier = max(attacker_stats['atk'] / defender_stats['def'], 0.2)
    hp_ratio_multiplier = max(defender_stats['cur_hp'] / defender_stats['max_hp'], 0.2)
    evade_multiplier = max(1 - (defender_stats['eva'] / attacker_stats['spd']), 0.1)
    luck_multiplier = max(1 - defender_stats['luck'] / attacker_stats['luck'], 5)
    charisma_multiplier = max(attacker_stats['chr'] / 10, 0.1)

    # Calculate the final damage
    final_damage = max(int(base_damage * attack_multiplier * hp_ratio_multiplier * evade_multiplier * luck_multiplier * charisma_multiplier), 0)

    # If final damage is 0, have a 1-10 chance of hitting a 1
    if final_damage == 0 and random.randint(1, 10) == 1:
        final_damage = 1

    return final_damage



def check_current_hp(user_id):
    try:
        cursor = level_conn.cursor()
        cursor.execute("SELECT cur_hp FROM users_rpg_stats WHERE user_id=?", (user_id,))
        result = cursor.fetchone()
        return result[0] if result else 0

    except sqlite3.Error as e:
        print(f"Error retrieving current HP for user {user_id}: {e}")
        return 0

def update_current_hp(user_id, new_hp):
    try:
        cursor = level_conn.cursor()
        cursor.execute("UPDATE users_rpg_stats SET cur_hp=? WHERE user_id=?", (new_hp, user_id))
        level_conn.commit()

    except sqlite3.Error as e:
        print(f"Error updating current HP for user {user_id}: {e}")

create_users_level_table(level_conn)

# Generate and insert experience values into the table
max_level = 100  # Adjust the maximum level as needed
experience_table = generate_experience_table(max_level)



async def updown_limits(self, ctx, asset):
    if asset in self.etfs:
        current_price = await get_etf_value(self.conn, int(asset))
    else:
        current_price = await get_stock_price(self, ctx, asset)


    lower_limit, upper_limit = calculate_option_range(self, current_price)

    return lower_limit, upper_limit, current_price

def set_current_city(user_id, city):
    try:
        with level_conn:
            level_conn.execute("INSERT OR REPLACE INTO users_rpg_cities (user_id, current_city, last_city, traveling, timestamp) VALUES (?, ?, ?, ?, ?)",
                         (user_id, city, get_current_city(user_id), "No", None))
    except sqlite3.Error as e:
        print(f"Error setting current city for user {user_id}: {e}")

def get_current_city(user_id):
    try:
        with level_conn:
            cursor = level_conn.execute("SELECT current_city FROM users_rpg_cities WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
            return result[0] if result else None
    except sqlite3.Error as e:
        print(f"Error getting current city for user {user_id}: {e}")
        return None

def set_last_city(user_id, city):
    try:
        with level_conn:
            level_conn.execute("UPDATE users_rpg_cities SET last_city = ? WHERE user_id = ?", (city, user_id))
    except sqlite3.Error as e:
        print(f"Error setting last city for user {user_id}: {e}")

def set_traveling_status(user_id, status):
    try:
        with level_conn:
            level_conn.execute("UPDATE users_rpg_cities SET traveling = ? WHERE user_id = ?", (status, user_id))
    except sqlite3.Error as e:
        print(f"Error setting traveling status for user {user_id}: {e}")

# Function to get the last travel timestamp
def get_last_travel_timestamp(self, user_id):
    return self.last_travel.get(user_id, datetime.min)

# Function to set the last travel timestamp
def set_last_travel_timestamp(self, user_id, timestamp):
    self.last_travel[user_id] = timestamp

def can_travel(user_id):
    last_timestamp = get_last_travel_timestamp(user_id)

    if last_timestamp:
        last_timestamp = datetime.strptime(last_timestamp, '%Y-%m-%d %H:%M:%S')  # Convert string to datetime

        # Calculate the time difference
        time_difference = datetime.now() - last_timestamp

        # Compare the time difference with the travel cooldown
        return time_difference >= calculate_travel_time()
    else:
        return True


def travel_to_city(user_id, destination_city):
    try:
        with level_conn:
            if can_travel(user_id):
                set_last_city(user_id, get_current_city(user_id))
                set_current_city(user_id, destination_city)
                set_traveling_status(user_id, "No")
                set_travel_timestamp(self, user_id)
                return True
            else:
                return False  # User cannot travel yet
    except sqlite3.Error as e:
        print(f"Error initiating travel for user {user_id}: {e}")
        return False


def add_city_tax(user_id, tax):
    city = get_current_city(user_id)
    city_funds = get_city_qse(city)

    try:
        cursor = level_conn.cursor()

        # Check if the city exists in user_city_stats
        cursor.execute("SELECT COUNT(*) FROM user_city_stats WHERE city = ?", (city,))
        city_exists = cursor.fetchone()[0] > 0

        if not city_exists:
            # City doesn't exist, create a new entry
            cursor.execute("""
                INSERT INTO user_city_stats (city, QSE, Resources, Stocks, ETPs)
                VALUES (?, ?, COALESCE(?, 0), COALESCE(?, 0), COALESCE(?, 0))
            """, (city, '0', None, None, None))
            level_conn.commit()

        # Update QSE amount in city statistics
        city_tax = city_funds + Decimal(tax)
        update_city_qse(city, str(city_tax))  # Convert the result back to text

    except sqlite3.Error as e:
        print(f"Error adding city tax: {e}")


# Function to update QSE amount in city statistics
def update_city_qse(city, new_qse_amount):
    try:
        cursor = level_conn.cursor()

        # Update QSE amount in city statistics
        cursor.execute("UPDATE user_city_stats SET QSE = ? WHERE city = ?", (new_qse_amount, city))
        level_conn.commit()

    except sqlite3.Error as e:
        print(f"Error updating QSE amount in city statistics: {e}")

# Function to get QSE amount from city statistics
def get_city_qse(city):
    try:
        cursor = level_conn.cursor()

        # Fetch QSE amount from city statistics
        cursor.execute("SELECT QSE FROM user_city_stats WHERE city = ?", (city,))
        result = cursor.fetchone()

        # Check if the result is not None before extracting QSE value
        if result is not None and result[0] is not None:
            qse_amount = Decimal(result[0])  # Assuming QSE is now stored as text
            return qse_amount
        else:
            return Decimal(0)

    except sqlite3.Error as e:
        print(f"Error fetching QSE amount from city statistics: {e}")
        return Decimal(0)

# Function to get city statistics
def get_city_stats(city):
    try:
        cursor = level_conn.cursor()

        # Fetch city statistics
        cursor.execute("SELECT * FROM user_city_stats WHERE city = ?", (city,))
        result = cursor.fetchone()

        return result

    except sqlite3.Error as e:
        print(f"Error fetching city statistics: {e}")
        return None

# Function to update city statistics
def update_city_stats(city, qse, resources, stocks, etps):
    try:
        cursor = level_conn.cursor()

        # Convert values to integers if they are numeric
        qse = Decimal(qse) if qse is not None else Decimal(0)
        resources = Decimal(resources) if resources is not None else Decimal(0)
        stocks = Decimal(stocks) if stocks is not None else Decimal(0)
        etps = Decimal(etps) if etps is not None else Decimal(0)

        # Update city statistics
        cursor.execute("""
            INSERT OR REPLACE INTO user_city_stats (city, QSE, Resources, Stocks, ETPs)
            VALUES (?, ?, ?, ?, ?)
        """, (city, str(qse), int(resources), int(stocks), int(etps)))

        level_conn.commit()

    except sqlite3.Error as e:
        print(f"Error updating city statistics: {e}")



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


async def boost_woodvale(self, ctx, booster):
    currency_conn = sqlite3.connect("currency_system.db")

    try:
        # Retrieve the list of underlying stocks and their quantities for Woodvale ETF
        with currency_conn:
            currency_cursor = currency_conn.cursor()
            etf_id = 7

            # Exclude "Om" stock from boosting
            currency_cursor.execute("""
                SELECT symbol, quantity
                FROM etf_stocks
                WHERE etf_id = ? AND symbol != "Om"
            """, (etf_id,))

            stocks = currency_cursor.fetchall()

            for stock_symbol, _ in stocks:
                await self.buy_stock_for_bot(ctx, stock_symbol, booster)
                print(f"Boosted Stock: {stock_symbol}")

    finally:
        currency_conn.close()

def is_allowed_user(*user_ids):
    async def predicate(ctx):
        if ctx.author.id not in user_ids:
            await ctx.send("You do not have the required permissions to use this command.")
            return False
        return True
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
        self.last_job_times = {}
        self.last_range_times = {}
        self.games = {}
        self.bot_address = "P3:03da907038"
        self.P3addrConn = sqlite3.connect("P3addr.db")
        self.ledger_conn = sqlite3.connect("p3ledger.db")
        self.reset_stock_limit_all.start()

        self.last_buyers = []
        self.last_sellers = []
        self.last_gamble = []
        self.last_travel = {}


        self.buy_timer_start = 0
        self.sell_timer_start = 0
        self.buy_item_timer_start = 0
        self.sell_item_timer_start = 0
        self.buy_etf_timer_start = 0
        self.sell_etf_timer_start = 0
        self.tax_command_timer_start = 0

        self.buy_stock_avg = []
        self.sell_stock_avg = []
        self.buy_etf_avg = []
        self.sell_etf_avg = []
        self.buy_item_avg = []
        self.sell_item_avg = []
        self.tax_command_avg = [0.1]

        self.casino_timer_start = 0
        self.reserve_timer_start = 0
        self.transfer_timer_start = 0
        self.transfer_avg = []
        self.casino_avg = []
        self.reserve_avg = []

        self.run_counter = 0

        self.cache = {}

        self.transaction_pool = []
        self.skipped_transactions = []
        self.transaction_lock = asyncio.Lock()
        self.db_semaphore = asyncio.Semaphore()
        self.total_pool = 0

        # IPO Config
        self.ipo_stocks = []
        self.ipo_price_limit = 100000


        # Circuit Breaker Config
        self.is_halted = False
        self.is_halted_order = False
        self.is_paused = False
        self.is_long_order = False
        self.contract_pool = []

        self.market_circuit_breaker =  False
        self.stock_circuit_breaker = False

        self.not_trading = []
        self.maintenance = ['secureharbor', 'p3:stable', 'roflstocks', 'GoldToken']
        self.stock_monitor = defaultdict(list)
#        self.ceo_stocks = [("partyscene", 607050637292601354), ("p3:bank", 930513222820331590), ("savage", 1147507029494202461)]
        self.ceo_stocks = {
            607050637292601354: ["partyscene"],
            1147507029494202461: ["savage", "citizen", "xfinichi", "nonsense", "singlesoon"],
            930513222820331590: ["p3:bank"]
            # Add more entries as needed
        }

        self.last_market_value = 0.0

        self.market_timeout = 300
        self.market_limit = 10
        self.stock_timeout = 300
        self.stock_limit = 100

        self.market_halts = 0
        self.stock_halts = 0


        self.cities = ["StellarHub", "TechnoMetra", "Quantumopolis", "CryptoVista"]
        self.safe_city = "StellarHub"
        self.etfs = ["1", "2", "3", "4", "6", "7", "8", "9", "10", "11", "12", "13"]


        self.UpDownPer = 250
        self.UpDownPerRange = 10 / 100.0



    async def is_trading_halted(self):
        if self.is_halted == True:
            return True
        else:
            return False


    async def is_trading_halted_stock(self, stock: str):
        halted_assets = self.not_trading + self.maintenance
        for i in halted_assets:
            if i == stock.lower():
                return True
        return False


    def is_staking_qse_genesis(self, user_id):
        try:
            cursor = self.conn.cursor()

            # Check if the user is staking a QSE Genesis NFT
            cursor.execute("SELECT COUNT(*) FROM user_stakes WHERE user_id=? AND nft='qse-genesis'", (user_id,))
            count = cursor.fetchone()[0]

            return count > 0

        except sqlite3.Error as e:
            print(f"An error occurred: {e}")
            return False

    def update_average_time(self, transaction_type: str, new_time: float):
        # Add the new time to the corresponding list
        if transaction_type == "buy_stock":
            self.buy_stock_avg.append(new_time)
        elif transaction_type == "sell_stock":
            self.sell_stock_avg.append(new_time)
        elif transaction_type == "buy_etf":
            self.buy_etf_avg.append(new_time)
        elif transaction_type == "sell_etf":
            self.sell_etf_avg.append(new_time)
        elif transaction_type == "casino":
            self.casino_avg.append(new_time)
        elif transaction_type == "reserve":
            self.reserve_avg.append(new_time)
        elif transaction_type == "buy_item":
            self.buy_item_avg.append(new_time)
        elif transaction_type == "sell_item":
            self.sell_item_avg.append(new_time)
        elif transaction_type == "transfer":
            self.transfer_avg.append(new_time)
        elif transaction_type == "tax_command":
            self.tax_command_avg.append(new_time)

    def calculate_average_time(self, transaction_type: str) -> float:
        # Combine all lists into one
        all_times = (
            self.buy_stock_avg +
            self.sell_stock_avg +
            self.buy_etf_avg +
            self.sell_etf_avg +
            self.casino_avg +
            self.reserve_avg +
            self.buy_item_avg +
            self.sell_item_avg +
            self.transfer_avg +
            self.tax_command_avg
            )


#        # Filter times based on transaction type
#        specific_times = getattr(self, f"{transaction_type}_avg", [])

        # Calculate and return the average time for the specific transaction type
#        return sum(specific_times) / len(specific_times) if specific_times else 0.0


        # Calculate and return the average time
        return sum(all_times) / len(all_times) if all_times else 0.0


    def calculate_average_time_type(self, transaction_type: str) -> float:
        # Combine all lists into one
        all_times = (
            self.buy_stock_avg +
            self.sell_stock_avg +
            self.buy_etf_avg +
            self.sell_etf_avg +
            self.casino_avg +
            self.reserve_avg +
            self.buy_item_avg +
            self.sell_item_avg +
            self.transfer_avg +
            self.tax_command_avg
            )


#        # Filter times based on transaction type
        specific_times = getattr(self, f"{transaction_type}_avg", [])

        # Calculate and return the average time for the specific transaction type
        return sum(specific_times) / len(specific_times) if specific_times else 0.0

    def calculate_tax_percentage(self, ctx, transaction_type: str) -> float:
        is_staking_qse_genesis = self.is_staking_qse_genesis(ctx.author.id)
        tax_rate = self.calculate_average_time(transaction_type)

        # Check if the user is staking_qse_genesis and apply a 10% discount
        if is_staking_qse_genesis:
            discount_percentage = 10
            tax_rate -= (tax_rate * discount_percentage / 100)

        # If tax_rate is still 0.0, set it to the default rate
        if tax_rate == 0.0:
            tax_rate = 1.75 / 100
        else:
            tax_rate /= 50  # Dividing by 10, assuming tax_rate is a percentage

        # Ensure tax_rate doesn't exceed 30%
        tax_rate = min(tax_rate, 0.30)

        return tax_rate



    @commands.command(name="transaction_metrics", help="Show average transaction speeds for each type.")
    async def show_transaction_metrics(self, ctx):
        transaction_types = [
            "buy_stock",
            "sell_stock",
            "buy_etf",
            "sell_etf",
            "casino",
            "reserve",
            "buy_item",
            "sell_item",
            "transfer",
            "tax_command"
        ]

        gas = self.calculate_tax_percentage(ctx, "sell_etf") * 100
        embed = Embed(title="Transaction Metrics", color=discord.Color.blue())

        total_transactions = 0
        total_average_time = 0.0

        for transaction_type in sorted(transaction_types, key=lambda x: self.calculate_average_time(x), reverse=True):
            avg_time = self.calculate_average_time_type(transaction_type)
            transaction_count = len(getattr(self, f"{transaction_type}_avg", []))
            transaction_count = max(transaction_count - 1, 0)  # Ensure non-negative count
            if transaction_count == 0:
                avg_time = 0.0

            total_transactions += transaction_count
            total_average_time += avg_time * transaction_count

            if transaction_count != 0:

                embed.add_field(
                    name=transaction_type.capitalize(),
                    value=f"Average Time: {avg_time:.5f}s\nTransaction Count: {(transaction_count + 1):,}",
                    inline=False
                )

        combined_average = total_average_time / total_transactions if total_transactions > 0 else 0.0

        if total_transactions != 0:
            total_transactions += 1

        embed.add_field(name="Combined Average Time:", value=f"{combined_average:.5f}s", inline=False)
        embed.add_field(name="Total Transactions:", value=f"{total_transactions:,}", inline=False)
        embed.add_field(name="Gas Fee:", value=f"{gas:,.2f}%")
        embed.set_footer(text="Combined average time is calculated across all transaction types.")

        await ctx.send(embed=embed)




    @tasks.loop(hours=1)  # Run every hour
    async def reset_stock_limit_all(self):
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

                channel_id = 1164250215239385129  # Replace with the desired channel ID
                channel = self.bot.get_channel(channel_id)
                await channel.send("Successfully reset daily stock buy limits for all users.")
            else:
                channel_id = 1164250215239385129  # Replace with the desired channel ID
                channel = self.bot.get_channel(channel_id)
        except sqlite3.Error as e:
            channel_id = 1164250215239385129 # Replace with the desired channel ID
            channel = self.bot.get_channel(channel_id)
            await channel.send(f"An error occurred: {str(e)} @Moderator")
        finally:
            # Close the database connections
            p3addr_conn.close()
            currency_conn.close()

    @commands.command(name="manual_reset_stock_limit", help="Manually reset daily stock buy limits for all users.")
    @is_allowed_user(930513222820331590, PBot)
    async def manual_reset_stock_limit(self, ctx):
        # Manually trigger the loop
        self.reset_stock_limit_all.restart()
        await ctx.send("Manual reset of daily stock buy limits initiated.")




    @tasks.loop(minutes=1.5)
    async def update_topstocks(self):
        try:
            channel_id = 1161735935944306808 # Replace with your actual channel ID
            message_id = 1161736160998072400  # Replace with your actual message ID
            channel = self.bot.get_channel(channel_id)

            if channel:
                cursor = self.conn.cursor()

                # Get the top 5 highest price stocks with available quantities
                cursor.execute("SELECT symbol, price, available FROM stocks WHERE available > 0 ORDER BY price DESC LIMIT 10")
                top_high_stocks = cursor.fetchall()

                # Get the top 5 lowest price stocks with available quantities
                cursor.execute("SELECT symbol, price, available FROM stocks WHERE available > 0 ORDER BY price ASC LIMIT 10")
                top_low_stocks = cursor.fetchall()

                # Create the embed
                embed = discord.Embed(title='Top 5 Highest and Lowest Price Stocks', color=discord.Color.blue())

                # Add fields for the top 5 highest price stocks
                for i, (symbol, price, available) in enumerate(top_high_stocks, start=1):
                    decimal_places = 8 if price < 0.01 else 2
                    embed.add_field(
                        name=f"High #{i}: {symbol}",
                        value=f"Price: {price:,.{decimal_places}f} $QSE\nAvailable: {available:,}",
                        inline=False
                    )

                # Add fields for the top 5 lowest price stocks
                for i, (symbol, price, available) in enumerate(top_low_stocks, start=1):
                    decimal_places = 8 if price < 0.01 else 2
                    embed.add_field(
                        name=f"Low #{i}: {symbol}",
                        value=f"Price: {price:,.{decimal_places}f} $QSE\nAvailable: {available:,}",
                        inline=False
                    )

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


    @commands.command(name="check_stock_supply", help="Check and update stock supply.")
    async def check_stock_supply(self, ctx):
        cursor = self.conn.cursor()

        # Get the total supply for each stock
        cursor.execute("SELECT symbol, available FROM stocks")
        stocks = cursor.fetchall()

        for stock in stocks:
            symbol, available = stock
            total_supply, available = await get_supply_info(self, ctx, symbol)

            # Get the total amount held by users
            cursor.execute("SELECT COALESCE(SUM(amount), 0) FROM user_stocks WHERE symbol=?", (symbol,))
            total_user_amount = cursor.fetchone()[0]

            # Calculate the total supply
            total_supply = total_user_amount + available

            # Update the total supply in the stocks table
            cursor.execute("UPDATE stocks SET total_supply=? WHERE symbol=?", (str(total_supply), symbol))
            cursor.execute("UPDATE stocks SET available=? WHERE symbol=?", (str(available), symbol))

        self.conn.commit()
        await ctx.send("Stock supply checked and updated.")


    @commands.command(name='test_stock_info')
    async def test_stock_info(self, ctx, symbol):
        ReserveHoldings = self.get_user_stock_amount(PBot, symbol)
        total_supply, locked = await get_supply_info(self, ctx, symbol)
        escrow_supply = await get_total_shares_in_orders(self, symbol)
        market = ReserveHoldings - escrow_supply
        circulating = total_supply - ReserveHoldings - escrow_supply - locked


        print(f"""
        PBot = {ReserveHoldings:,.0f}
        Locked = {locked:,.0f}
        total = {total_supply:,.0f}
        escrow = {escrow_supply:,.0f}
        market = {market:,.0f}
        circulating = {circulating:,.0f}


        """)


    @commands.command(name='servers', help='Check how many servers the bot is in.')
    async def servers(self, ctx):
        server_list = "\n".join([guild.name for guild in self.bot.guilds])
        server_count = len(self.bot.guilds)

        if server_count == 0:
            await ctx.send("I am not in any servers.")
        else:
            await ctx.send(f"I am in {server_count} server(s):\n{server_list}")


#Game Help

    @commands.command(name='game-help', aliases=["help"], help='Display available commands for Discord $QSE.')
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





#NFT Staking


    @commands.command(name="connect_wallet", aliases=["connect"])
    async def connect_wallet(self, ctx, wallet_address: str):
        try:
            user_id = ctx.author.id

            # Assuming you have a connection to the database
            conn = sqlite3.connect("currency_system.db")
            addrDB = sqlite3.connect("P3addr.db")
            cursor = conn.cursor()

            p3_address = get_p3_address(addrDB, user_id)

            # Check if the user already exists in the table
            cursor.execute("SELECT * FROM user_wallets WHERE user_id=?", (user_id,))
            existing_user = cursor.fetchone()

            if existing_user:
                # Update existing user's wallet and P3 address
                cursor.execute("UPDATE user_wallets SET wallet_address=?, p3_address=? WHERE user_id=?", (wallet_address, p3_address, user_id))
                conn.commit()
                await ctx.send("Wallet successfully updated.")
            else:
                # Insert new user
                cursor.execute("INSERT INTO user_wallets (user_id, wallet_address, p3_address) VALUES (?, ?, ?)", (user_id, wallet_address, p3_address))
                conn.commit()
                await ctx.send("Wallet successfully connected.")

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred: {e}")

        finally:
            # Close the connection
            addrDB.close()
            conn.close()



    @commands.command(name="check_wallet")
    async def check_wallet(self, ctx):
        user_id = ctx.author.id

        # Assuming you have a connection to the database
        conn = sqlite3.connect("currency_system.db")
        cursor = conn.cursor()

        # Check if the user exists in the table
        cursor.execute("SELECT wallet_address FROM user_wallets WHERE user_id=?", (user_id,))
        wallet_address = cursor.fetchone()

        if wallet_address:
            await ctx.send(f"Your stored wallet address is: {wallet_address[0]}")
        else:
            await ctx.send("You haven't connected a wallet yet. Use `!connect_wallet <wallet_address>` to connect your wallet.")

        # Close the connection
        conn.close()


    @commands.command(name="stake")
    async def stake(self, ctx, nft: str, tokenid: str):
        try:
            user_id = ctx.author.id

            # Assuming you have a connection to the database
            conn = sqlite3.connect("currency_system.db")
            cursor = conn.cursor()

            accepted_nft_names = ["penthouse-og", "penthouse-legendary", "stake-booster", "poly-the-parrot", "qse-genesis", "realtor-license", "urban-penthouse"]

            if nft.lower() not in accepted_nft_names:
                await ctx.send(f"We currently only accept {', '.join(accepted_nft_names[:-1])}, and {accepted_nft_names[-1]} Penthouse Staking during the test phase.\nType: !stake {'/'.join(accepted_nft_names)} <tokenid>\n\nTo find your tokenid, check your NFT on OpenSea.")
                return


            # Check if the user exists in the user_wallets table
            cursor.execute("SELECT wallet_address FROM user_wallets WHERE user_id=?", (user_id,))
            wallet_address = cursor.fetchone()

            if not wallet_address:
                await ctx.send("You haven't connected a wallet yet. Use `!connect_wallet <wallet_address>` to connect your wallet.")
                return

            # Separate connection variable for the finally block
            finally_conn = sqlite3.connect("currency_system.db")
            finally_cursor = finally_conn.cursor()

            try:
                # Check if the user has already staked with this NFT and tokenid
                finally_cursor.execute("SELECT * FROM user_stakes WHERE user_id=? AND nft=? AND tokenid=?", (user_id, nft, tokenid))
                existing_stake = finally_cursor.fetchone()

                if existing_stake:
                    await ctx.send("You have already staked with this NFT and tokenid.")
                else:
                    # Insert new stake with timestamp
                    current_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                    finally_cursor.execute("INSERT INTO user_stakes (user_id, nft, tokenid, stake_timestamp) VALUES (?, ?, ?, ?)", (user_id, nft, tokenid, current_timestamp))
                    finally_conn.commit()
                    await ctx.send("Stake successful...We will validate and verify your stake shortly.")

            except sqlite3.Error as e:
                await ctx.send(f"An error occurred: {e}")

            finally:
                # Close the connection in the finally block
                finally_conn.close()

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred: {e}")

        finally:
            # Close the connection in the outer finally block
            conn.close()


    @commands.command(name="stake_stats")
    async def stake_stats(self, ctx):
        try:
            user_id = ctx.author.id

            # Assuming you have a connection to the database
            conn = sqlite3.connect("currency_system.db")
            cursor = conn.cursor()

            # Get the count of each item staked by the user
            cursor.execute("SELECT nft, COUNT(*) FROM user_stakes WHERE user_id=? GROUP BY nft", (user_id,))
            stake_counts = cursor.fetchall()

            if not stake_counts:
                await ctx.send("You haven't staked any items yet.")
                return

            # Format and send the result
            result_str = "Stake Stats:\n"
            for nft, count in stake_counts:
                result_str += f"{nft}: {count} items\n"

            await ctx.send(result_str)

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred: {e}")

        finally:
            # Close the connection
            conn.close()





    @commands.command(name="stake_rewards")
    async def stake_rewards(self, ctx):
        try:
            user_id = ctx.author.id

            # Assuming you have a connection to the database
            conn = sqlite3.connect("currency_system.db")
            cursor = conn.cursor()

            # Get the count of each item staked by the user
            cursor.execute("SELECT nft, COUNT(*) FROM user_stakes WHERE user_id=? GROUP BY nft", (user_id,))
            stake_counts = cursor.fetchall()

            if not stake_counts:
                await ctx.send("You haven't staked any items yet.")
                return

            # Calculate rewards based on staked items
            base_rewards = {
                "penthouse-legendary": 750_000_000_000,  # 100 billion uPPN per week
                "penthouse-og": 50_000_000_000,
                "poly-the-parrot": 25_000_000_000,   # 30 billion uPPN per week
                "realtor-license": 100_000_000_000,
                "urban-penthouse": 45_000_000_000,
                "stake-booster": 0.10,  # 10% bonus to overall staking rewards
                "qse-genesis": 0.75
            }

            total_rewards = 0

            # Calculate rewards and build result string
            result_str = "Stake Rewards:\n"
            for nft, count in stake_counts:
                if nft in base_rewards and nft != "stake-booster" and nft != "qse-genesis":
                    base_reward = base_rewards[nft] * count
                    result_str += f"{nft}: {count} items, Weekly Rewards: {base_reward:,.2f} uPPN\n"
                    total_rewards += base_reward

            # Check for stake-boosters and apply the bonus
            stake_booster_count = next((count for nft, count in stake_counts if nft == "stake-booster"), 0)
            bonus_multiplier = base_rewards.get("stake-booster", 1.0)
            bonus_rewards = total_rewards * bonus_multiplier * stake_booster_count
            total_rewards += bonus_rewards

            # Check for QSE Genesis and apply the 75% boost
            qse_genesis_count = next((count for nft, count in stake_counts if nft == "qse-genesis"), 0)
            qse_genesis_boost = base_rewards.get("qse-genesis", 0.0)
            total_rewards += total_rewards * qse_genesis_boost * qse_genesis_count

            result_str += f"\nStake Booster Bonus: {bonus_rewards:,.2f} uPPN\n"
            result_str += f"Total Weekly Rewards: {total_rewards:,.2f} uPPN"

            # Send the result in an embed
            color = discord.Color.green()
            embed = Embed(title="Stake Rewards", description=result_str, color=color)
            await ctx.send(embed=embed)

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred: {e}")

        finally:
            # Close the connection
            conn.close()


    @commands.command(name="distribute_stake_rewards")
    @is_allowed_user(930513222820331590, PBot)
    async def distribute_stake_rewards(self, ctx):
        try:
            user_id = PBot

            # Assuming you have a connection to the database
            conn = sqlite3.connect("currency_system.db")
            cursor = conn.cursor()

            # Calculate total rewards
            base_rewards = {
                "penthouse-legendary": 750_000_000_000,  # 100 billion uPPN per week
                "penthouse-og": 50_000_000_000,
                "poly-the-parrot": 25_000_000_000,   # 30 billion uPPN per week
                "realtor-license": 100_000_000_000,
                "urban-penthouse": 45_000_000_000,
                "stake-booster": 0.10,  # 10% bonus to overall staking rewards
                "qse-genesis": 0.75
            }

            cursor.execute("SELECT nft, COUNT(*) FROM user_stakes GROUP BY nft")
            total_stake_counts = cursor.fetchall()

            total_rewards = sum(base_rewards[nft] * count for nft, count in total_stake_counts if nft in base_rewards and nft != "stake-booster" and nft != "qse-genesis")

            weekly_rewards = total_rewards * 7

            # Fetch all users who have staked
            cursor.execute("SELECT DISTINCT user_id FROM user_stakes")
            stakers = cursor.fetchall()

            # Distribute rewards to each staker
            for staker_id in stakers:
                staker_id = staker_id[0]  # Extract user_id from the tuple

                # Calculate staker's rewards based on their stakes
                cursor.execute("SELECT nft, COUNT(*) FROM user_stakes WHERE user_id=? GROUP BY nft", (staker_id,))
                staker_stake_counts = cursor.fetchall()
                staker_rewards = 0

                for nft, count in staker_stake_counts:
                    if nft in base_rewards and nft != "stake-booster":
                        base_reward = base_rewards[nft] * count
                        staker_rewards += base_reward

                # Check for stake-boosters and apply the bonus
                stake_booster_count = next((count for nft, count in staker_stake_counts if nft == "stake-booster"), 0)
                bonus_multiplier = base_rewards.get("stake-booster", 1.0)
                bonus_rewards = staker_rewards * bonus_multiplier * stake_booster_count
                staker_rewards += bonus_rewards

                # Check for QSE Genesis and apply the 75% boost
                qse_genesis_count = next((count for nft, count in staker_stake_counts if nft == "qse-genesis"), 0)
                qse_genesis_boost = base_rewards.get("qse-genesis", 0.0)
                staker_rewards += staker_rewards * qse_genesis_boost * qse_genesis_count

                # Send rewards to the staker using the modified send_rewards function
                await self.send_rewards(ctx, staker_id, staker_rewards)

            await ctx.send(f"Weekly rewards distributed: {weekly_rewards:,.2f} uPPN to all stakers.")

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred: {e}")

        finally:
            # Close the connection
            conn.close()

    async def send_rewards(self, ctx, staker_id, staker_rewards):
        bot_user_id = 1092870544988319905  # Replace with your bot's user ID
        bot_p3_address = "P3:03da907038"  # Replace with your bot's P3 address

        # Check if the bot has enough funds
        bot_balance = get_user_balance(self.conn, bot_user_id)
        if bot_balance < staker_rewards:
            await ctx.send(f"Error: Bot doesn't have enough funds to send rewards to user {staker_id}.")
            return

        # Deduct rewards from the bot's balance
        await update_user_balance(self.conn, bot_user_id, float(bot_balance) - float(staker_rewards))

        # Add rewards to the staker's balance
        await update_user_balance(self.conn, staker_id, float(get_user_balance(self.conn, staker_id)) + float(staker_rewards))

        # Log the transfer
        await log_transfer(self, ledger_conn, ctx, bot_p3_address, get_user_id(self.P3addrConn, bot_p3_address), staker_id, staker_rewards)

        await ctx.send(f"Successfully sent {staker_rewards:,.2f} $QSE rewards to user {get_p3_address(self.P3addrConn, staker_id)}.")



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



# BANK


    @commands.command(name="Open_Bank")
    async def open_bank(self, ctx, bank_name: str):
        await create_bank(self, bank_name)



    @commands.command(name="debug_bank")
    async def debug_bank(self, ctx):
        await show_all_banks(self)


    @commands.command(name="bank")
    async def bank(self, ctx, command):
        if command.lower() == "create":
            user_balance = get_user_balance(self.conn, ctx.author.id)
            if user_balance > 100_000:
                result = await create_user_account(self, ctx, 1)
                if result:
                    await update_user_balance(self.conn, ctx.author.id, user_balance - 100000)
                    await ctx.send(f"{result}")
                    return
                return
            else:
                await ctx.send("You need 100,000 QSE to open an account")
                return
        elif command.lower() == "account":
            result = await view_account(self, ctx,  1)
            if result:
                await ctx.send(embed=result)
        elif command.lower() == "deposit":
            user_balance = get_user_balance(self.conn, ctx.author.id)
            await ctx.send(f"Current Balance: {user_balance:,.0f}")
            await ctx.send("How much QSE would you like to deposit?")

            def check(msg):
                return msg.author == ctx.author and msg.channel == ctx.channel

            # Wait for user input
            response = await self.bot.wait_for('message', check=check, timeout=60)  # Adjust timeout as needed

            # Parse the user input
            amount = float(response.content)
            if amount < 100_000_000:
                await ctx.send("Deposit must be greater than 100,000,000 QSE")
                return
            if user_balance < amount:
                await ctx.send(f"{amount:,.0f} greater than current balance of {user_balance:,.0f}")
                return
            else:
                PBotAddr = get_p3_address(self.P3addrConn, PBot)
                await self.give_addr(ctx, PBotAddr, Decimal(amount), False)
                result = await deposit_qse(self, ctx, 1, amount)
                if result:
                    await ctx.send(result)
        elif command.lower() == "withdrawal":
            result = await view_account(self, ctx,  1)
            if result:
                await ctx.send(embed=result)
            await ctx.send("How much QSE would you like to withdrawal?")

            def check(msg):
                return msg.author == ctx.author and msg.channel == ctx.channel

            # Wait for user input
            response = await self.bot.wait_for('message', check=check, timeout=60)  # Adjust timeout as needed

            # Parse the user input
            amount = float(response.content)
            if amount < 100_000_000:
                await ctx.send("withdrawal must be greater than 100,000,000 QSE")
                return
            result = await withdraw_qse(self, ctx, 1, Decimal(amount))
            if result:
                await ctx.send(result)
                return
        else:
            return

    @commands.command(name="top_wealth", help="Show the top 10 wealthiest users.")
    async def top_wealth(self, ctx):
        cursor = self.conn.cursor()
        P3addrCursor = self.P3addrConn.cursor()

        await ctx.message.delete()

        # Get the top 10 wealthiest users, sorting them by total wealth (balance + stock value + ETF value + metal value)
        cursor.execute("""
            SELECT users.user_id,
                   (users.balance + IFNULL(total_stock_value, 0) + IFNULL(total_etf_value, 0) + IFNULL(total_metal_value, 0)) AS total_wealth
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
            LEFT JOIN (
                SELECT user_id, SUM(items.price * inventory.quantity) AS total_metal_value
                FROM inventory
                LEFT JOIN items ON inventory.item_id = items.item_id
                WHERE items.item_name IN ('Copper', 'Platinum', 'Gold', 'Silver', 'Lithium')
                GROUP BY user_id
            ) AS user_metal_data ON users.user_id = user_metal_data.user_id
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
            vanity_address = get_vanity_address(self.P3addrConn, user_id)
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


    @commands.command(name='top_escrow_users', help='Show the top 10 users by total amount of shares in escrow.')
    async def top_escrow_users(self, ctx):
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT user_id, SUM(quantity) as total_shares
                FROM limit_orders
                WHERE order_type = 'sell' OR order_type = 'buy'
                GROUP BY user_id
                ORDER BY total_shares DESC
                LIMIT 10
            """)

            top_users = cursor.fetchall()

            if top_users:
                embed = discord.Embed(
                    title="Top 10 Users by Total Shares in Escrow",
                    color=discord.Color.gold()
                )

                for rank, (user_id, total_shares) in enumerate(top_users, start=1):
                    p3addr = get_p3_address(self.P3addrConn, user_id)

                    embed.add_field(
                        name=f"{rank}. {p3addr}",
                        value=f"Total Shares: {total_shares:,.0f}",
                        inline=False
                    )

                await ctx.send(embed=embed)
            else:
                await ctx.send("No users found with shares in escrow.")

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while fetching top escrow users: {e}")


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
            result = await get_etf_value(self.conn, etf_id)
            if result:
                etf_value = result

                # Get the number of holders for the ETF
                cursor.execute("SELECT COUNT(DISTINCT user_id) FROM user_etfs WHERE etf_id = ? AND quantity > 0.0", (etf_id,))
                num_holders = cursor.fetchone()[0]

                # Get the top holders of the ETF with non-zero quantity
                cursor.execute("""
                    SELECT user_id, quantity
                    FROM user_etfs
                    WHERE etf_id = ? AND quantity > 0.0
                    ORDER BY quantity DESC
                    LIMIT 10
                """, (etf_id,))
                top_holders = cursor.fetchall()



                # Calculate the total value of all held ETFs by users
                cursor.execute("SELECT SUM(quantity) FROM user_etfs WHERE etf_id = ? AND quantity > 0.0", (etf_id,))
                total_value = cursor.fetchone()[0] * etf_value

                embed = Embed(title=f"ETF Metrics for ETF ID {etf_id}", color=discord.Color.green())
                embed.add_field(name="Current Value", value=f"{etf_value:,.2f} $QSE", inline=False)
                embed.add_field(name="Number of Holders", value=num_holders, inline=False)

                if top_holders:
                    top_holders_str = "\n".join([f"{generate_crypto_address(user_id)} - {quantity:,.2f} shares" for user_id, quantity in top_holders])
                    embed.add_field(name="Top Holders", value=top_holders_str, inline=False)
                else:
                    embed.add_field(name="Top Holders", value="No one currently holds shares in this ETF.", inline=False)

                # Include the total value in the footer
                embed.set_footer(text=f"Total Value of Held ETFs: {total_value:,.2f} $QSE")

                await ctx.send(embed=embed)

            else:
                await ctx.send(f"ETF with ID {etf_id} does not exist.")

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while fetching ETF information: {e}")


# Currency Tools

    @commands.command(name="report", help="Report bugs or issues with the bot.")
    async def report(self, ctx, *, issue=None):
        if issue is None:
            await ctx.send("Please provide an issue in quotations. Example: `!report \"There is an issue with...\"`")
            return

        # Get timestamp in EST
        timestamp_est = ctx.message.created_at.astimezone(timezone(timedelta(hours=-5)))

        # Create a link to the message
        message_link = f"https://discord.com/channels/{ctx.guild.id}/{ctx.channel.id}/{ctx.message.id}"

        # Notify user in an embed
        embed = discord.Embed(
            title="Report Submitted",
            description=f"Thank you for reporting the issue! An admin will check it at the earliest convenience.",
            color=discord.Color.blue()
        )
        embed.add_field(name="Issue", value=issue, inline=False)
        embed.add_field(name="Timestamp (EST)", value=timestamp_est.strftime("%Y-%m-%d %H:%M:%S"), inline=False)
        embed.add_field(name="Report Link", value=message_link, inline=False)

        # Send the embed to the channel
        await ctx.send(embed=embed)

        # Send report to admin
        admin_user_id = 930513222820331590  # Replace with your admin user ID
        admin = await self.bot.fetch_user(admin_user_id)
        admin_message = f"New report from {ctx.author.name} ({ctx.author.id}):\n\n**Issue:** {issue}\n**Timestamp (EST):** {timestamp_est.strftime('%Y-%m-%d %H:%M:%S')}\n**Report Link:** {message_link}"
        await admin.send(admin_message)
        await admin.send(embed=embed)

        await ctx.send("Your report has been submitted. Thank you!")

    async def handle_burn_transaction(self, ctx, sender_id, amount):
        # Deduct the tokens from the sender's balance (burn)
        await update_user_balance(self.conn, sender_id, get_user_balance(self.conn, sender_id) - amount)
        await update_user_balance(self.conn, PBot, get_user_balance(self.conn, PBot) - amount)

        # Log the burn transfer
        await log_transfer(self, ledger_conn, ctx, ctx.author.name, "p3:0x0000burn", get_user_id(self.P3addrConn, self.bot_address), amount, is_burn=True)
        await log_transfer(self, ledger_conn, ctx, ctx.bot.user.name, "p3:0x0000burn", get_user_id(self.P3addrConn, self.bot_address), amount, is_burn=True)

        # Calculate ticket amount
        is_staking_qse_genesis = self.is_staking_qse_genesis(sender_id)
        if is_staking_qse_genesis:
            ticket_amount = (amount * 2.5)
        else:
            ticket_amount = amount

        # Update ticket data
        ticket_data = await get_ticket_data(self.conn, sender_id)
        if ticket_data is None:
            await update_ticket_data(self.conn, sender_id, ticket_amount, int(time.time()))
        else:
            ticket_quantity, _ = ticket_data
            await update_ticket_data(self.conn, sender_id, ticket_quantity + ticket_amount, int(time.time()))

        # Send response
        await ctx.send(f"{amount:,.0f} $QSE has been burned, received {ticket_amount:,.0f} Lottery tickets")
        print(f"{get_p3_address(self.P3addrConn, sender_id)} burned {amount:,.0f} $QSE Token")



    @commands.command(name="give", help="Give a specified amount of $QSE to another user.")
    async def give_addr(self, ctx, target, amount: int, verbose: bool = True):
        sender_id = ctx.author.id
        sender_balance = get_user_balance(self.conn, sender_id)

        if amount <= 0:
            return

        if sender_balance < amount:
            await ctx.send(f"{ctx.author.mention}, you don't have enough $QSE to give. Your current balance is {sender_balance:,.2f} $QSE.")
            return

        # Check if the target address is the burn address
        if target.lower() == "p3:0x0000burn":
            # Deduct the tokens from the sender's balance (burn)
            await self.handle_burn_transaction(ctx, sender_id, amount)
            return

        if target.startswith("P3:"):
            p3_address = target
            user_id = get_user_id(self.P3addrConn, p3_address)

            if not user_id:
                await ctx.send("Invalid or unknown P3 address.")
                return
        else:
            await ctx.send("Please provide a valid P3 address.")
            return

        recipient_balance = get_user_balance(self.conn, user_id)

        # Apply tax for transfers
        transfer_amount = amount

        if transfer_amount > 9_000_000_000_000_000_000:
            chunks = (int(transfer_amount) + 99_999_999_999_999_999_999) // 9_000_000_000_000_000_000
            chunk_amount = int(transfer_amount) // chunks

        else:
            chunks = 1
            chunk_amount = transfer_amount

        # Deduct the total amount from the sender's balance
        await update_user_balance(self.conn, sender_id, sender_balance - transfer_amount)

        # Add the total transfer amount to the recipient's balance
        await update_user_balance(self.conn, user_id, recipient_balance + transfer_amount)

        # Log the transfer for each chunk
        for _ in range(chunks):
            await log_transfer(self, ledger_conn, ctx, ctx.author.name, target, user_id, chunk_amount)

        if verbose:
            await ctx.send(f"{ctx.author.mention}, you have successfully given {amount:,.0f} $QSE to {target}.")
        return




    @commands.command(name="daily", help="Claim your daily $QSE.")
    async def daily(self, ctx):
        await check_store_addr(self, ctx)
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

                # Deduct the amount from the bot's balance
                bot_balance = get_user_balance(self.conn, self.bot.user.id)
                await update_user_balance(self.conn, self.bot.user.id, bot_balance - amount)

                # Log the transfer from the bot to the user
                await log_transfer(self, ledger_conn, ctx, "P3 Bot", ctx.author.name, user_id, amount)

                await update_user_balance(self.conn, user_id, new_balance)
                await add_experience(self, self.conn, ctx.author.id, 10, ctx)
                await ctx.send(f"{ctx.author.mention}, you have claimed {amount:,.2f} $QSE. Your new balance is: {new_balance:,.2f} $QSE.\nAwarded 10xp")
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
        formatted_balance = f"**{formatted_balance}** $QSE"

        embed = discord.Embed(
            title="Balance",
            description=f"Balance for {P3Addr}:",
            color=discord.Color.blue()
        )
        embed.set_thumbnail(url="https://mirror.xyz/_next/image?url=https%3A%2F%2Fimages.mirror-media.xyz%2Fpublication-images%2F8XKxIUMy9CE8zg54-ZsP3.png&w=828&q=75")  # Add your own coin icon URL
        embed.add_field(name="$QSE", value=formatted_balance, inline=False)
        embed.set_footer(text="Thank you for using our currency system!")

        await ctx.send(embed=embed)

    @commands.command(name="add")
    @is_allowed_user(930513222820331590, PBot)
    async def add(self, ctx, amount: int):
        await ctx.message.delete()
        user_id = ctx.author.id
        current_balance = get_user_balance(self.conn, user_id)
        new_balance = current_balance + amount
        await update_user_balance(self.conn, user_id, new_balance)
        await ctx.send(f"{ctx.author.mention}, you have added {amount:,.2f} $QSE. Your new balance is: {new_balance:,.2f} $QSE.")




# Debug Start




    @commands.command(name="addr_metric", help="Show metrics for a P3 address.")
    async def addr_metric(self, ctx, target_address):
        # Connect to P3 address database

        # Get user_id associated with the target address
        user_id = get_user_id(self.P3addrConn, target_address)

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
            self.P3addrConn.close()
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

            if symbol.lower() == "roflstocks":
                if isinstance(price_str, str):
                    formatted_price = price_str.replace(",", "")
                    price = float(formatted_price)
                else:
                    price = float(price_str)
            else:
                if isinstance(price_str, float):
                    price_str = str(price_str)
                    price_str = price_str.replace(",", "")
                price = float(price_str.replace(",", ""))


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
            current_price = await get_stock_price(self, ctx, symbol)
            if symbol == "ROFLStocks":
                formatted_current_price = '{:,.11f}'.format(current_price)
            else:
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
        formatted_total_buys = '{:,.0f}'.format(int(total_buys))
        formatted_total_sells = '{:,.0f}'.format(int(total_sells))
        total_buy_volume, total_sell_volume, total_volume = await calculate_volume(symbol, interval='daily')
        daily_volume = total_volume or 0
        daily_volume_value = daily_volume * current_price or 0
        daily_buy_volume = total_buy_volume or 0
        daily_buy_volume_value = daily_buy_volume * current_price or 0
        daily_sell_volume = total_sell_volume or 0
        daily_sell_volume_value = daily_sell_volume * current_price or 0
        min_price = await calculate_min_price(self, ctx, symbol)
        max_price = await calculate_max_price(self, ctx, symbol)
        min_price = float(min_price)
        max_price = float(max_price)
        opening_price, closest_price, interval_change = await get_stock_price_interval(self, ctx, symbol, interval='daily')
        if opening_price != 0:
            opening_change = (interval_change / opening_price) * 100
        else:
            opening_change = 10

        # Create an embed to display the statistics
        embed = discord.Embed(
            title=f"Stock Statistics for {symbol}📊",
            color=discord.Color.blue()
        )
        circulatingValue = circulating_supply * current_price
        formatet_circulating_value = '{:,}'.format(int(circulatingValue))
#        embed.add_field(name="Current Price", value=f"{formatted_current_price} $QSE", inline=False)
        avg_buy, avg_sell = await calculate_average_prices_by_symbol(self, symbol)
        avg_price = (avg_buy + avg_sell) / 2
        buy_price = (avg_price * 0.05) + avg_price
        sell_price = avg_price - (avg_price * 0.03)
        embed.add_field(name="Average Price", value=f"{avg_price:,.11f} $QSE🔁", inline=False)
        embed.add_field(name="Buy Price", value=f"{buy_price:,.11f} $QSE🔁", inline=False)
        embed.add_field(name="Sell Price", value=f"{sell_price:,.11f} $QSE🔁", inline=False)
        embed.add_field(name="Daily Volume:", value=f"{daily_volume:,} shares", inline=False)
        embed.add_field(name="Daily Volume Value:", value=f"{daily_volume_value:,.2f} $QSE", inline=False)
        embed.add_field(name="Daily Buy Volume:", value=f"{daily_buy_volume:,.0f} shares 📈", inline=False)
        embed.add_field(name="Daily Buy Volume Value:", value=f"{daily_buy_volume_value:,.2f} $QSE 📈", inline=False)
        embed.add_field(name="Daily Sell Volume:", value=f"{daily_sell_volume:,.0f} shares 📉", inline=False)
        embed.add_field(name="Daily Sell Volume Value:", value=f"{daily_sell_volume_value:,.2f} $QSE 📉", inline=False)
        embed.add_field(name="Total Buys💵", value=f"{formatted_total_buys} $QSE ({total_quantity_buys:,} buys📈)", inline=False)
        embed.add_field(name="Total Sells💵", value=f"{formatted_total_sells} $QSE ({total_quantity_sells:,} sells📉)", inline=False)
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
                embed.add_field(name="Total Buys", value=f"{formatted_total_buys} $QSE ({total_quantity_buys:,} buys)", inline=False)
                embed.add_field(name="Total Sells", value=f"{formatted_total_sells} $QSE ({total_quantity_sells:,} sells)", inline=False)
                embed.add_field(name="Average Price", value=f"{formatted_average_price} $QSE", inline=False)
                embed.add_field(name="Current ETF Value", value=f"{formatted_etf_value} $QSE", inline=False)
                embed.add_field(name="Valuation", value=valuation_label, inline=False)

                await ctx.send(embed=embed)

            else:
                await ctx.send(f"ETF {etf_id} not found.")

            # Close the database connections
            ledger_conn.close()
            currency_conn.close()

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred: {str(e)}")




    @commands.command(name="stock_chart", aliases=["chart"], help="Display a price history chart with technical indicators for a stock.")
    async def stock_chart(self, ctx, stock_symbol, time_period=None, rsi_period=None, sma_period=None, ema_period=None, price_debug: bool = False):
        if self.is_long_order:
            self.contract_pool.append(("chart", self, ctx, stock_symbol, time_period, rsi_period, sma_period, ema_period, price_debug))
            return
        if stock_symbol.lower() in self.ipo_stocks:
            embed = discord.Embed(description="Chart unavailable while stock in IPO", color=discord.Color.red())
            await ctx.send(embed=embed)
            return
        self.tax_command_timer_start = timeit.default_timer()
        async with self.db_semaphore:
            async with self.transaction_lock:
                try:
                    avg_buy, avg_sell = await calculate_average_prices_by_symbol(self, stock_symbol)
                    avg_price = (avg_buy + avg_sell) / 2
                    await self.change_stock_price(ctx, stock_symbol, avg_price)
                    # Connect to the currency_system database
                    currency_conn = sqlite3.connect("currency_system.db")
                    currency_cursor = currency_conn.cursor()

                    # Check if the given stock symbol exists
                    currency_cursor.execute("SELECT symbol, price FROM stocks WHERE symbol=?", (stock_symbol,))
                    stock = currency_cursor.fetchone()

                    if stock:
                        stock_symbol, current_price = stock

                        # Connect to the ledger database
                        with sqlite3.connect("p3ledger.db") as ledger_conn:
                            ledger_cursor = ledger_conn.cursor()
                            # Retrieve buy/sell transactions for the stock from the ledger
                            transactions = ledger_cursor.execute("""
                                SELECT timestamp, action, price
                                FROM stock_transactions
                                WHERE symbol=? AND (action='Buy Stock' OR action='Sell Stock')
                                ORDER BY timestamp
                            """, (stock_symbol,))

                        if transactions:
                            # Separate buy and sell transactions
                            buy_prices = []
                            sell_prices = []
                            all_prices = []

                            for timestamp_str, action, price_str in transactions:
                                if stock_symbol.lower() == "roflstocks":
                                    formatted_price = str(price_str).replace(",", "") if isinstance(price_str, str) else price_str
                                    price = float(formatted_price)
                                else:
                                    formatted_price = str(price_str).replace(",", "")
                                    price = float(formatted_price)
                                datetime_obj = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")  # Parse timestamp string
                                all_prices.append((datetime_obj, price, action))

                                if action == 'Buy Stock':
                                    buy_prices.append((datetime_obj, price))
                                elif action == 'Sell Stock':
                                    sell_prices.append((datetime_obj, price))

                            # Sort all prices by timestamp
                            all_prices.sort(key=lambda x: x[0])

                            # Extract datetime objects and prices for buy and sell transactions
                            all_prices_np = np.array(all_prices)
                            timestamps, prices, actions = all_prices_np[:, 0], all_prices_np[:, 1], all_prices_np[:, 2]

                            # Extract datetime objects and prices for buy and sell transactions
                            buy_prices_np = np.array(buy_prices)
                            buy_timestamps, buy_prices = buy_prices_np[:, 0], buy_prices_np[:, 1]

                            sell_prices_np = np.array(sell_prices)
                            sell_timestamps, sell_prices = sell_prices_np[:, 0], sell_prices_np[:, 1]

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

                            buy_mask = buy_timestamps >= start_date
                            sell_mask = sell_timestamps >= start_date

                            buy_prices, buy_timestamps = buy_prices[buy_mask], buy_timestamps[buy_mask]
                            sell_prices, sell_timestamps = sell_prices[sell_mask], sell_timestamps[sell_mask]

                            # Calculate average buy and sell prices
                            average_buy_price = np.mean(buy_prices) if buy_prices.size else 0
                            average_sell_price = np.mean(sell_prices) if sell_prices.size else 0

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
                            if buy_prices.size >= int(rsi_period):
                                buy_prices = buy_prices.astype('double')  # Convert to double type
                                rsi = talib.RSI(buy_prices, timeperiod=int(rsi_period))
                                rsi_timestamps = buy_timestamps[-len(rsi):]  # Match RSI timestamps
                            else:
                                rsi = None  # Insufficient data for RSI
                                rsi_timestamps = []

                            # Calculate Simple Moving Average (SMA)
                            if buy_prices.size >= int(sma_period):
                                sma = talib.SMA(buy_prices, timeperiod=int(sma_period))
                                sma_timestamps = buy_timestamps[-len(sma):]  # Match SMA timestamps
                            else:
                                sma = None  # Insufficient data for SMA
                                sma_timestamps = []

                            # Calculate Exponential Moving Average (EMA)
                            if buy_prices.size >= int(ema_period):
                                ema = talib.EMA(buy_prices, timeperiod=int(ema_period))
                                ema_timestamps = buy_timestamps[-len(ema):]  # Match EMA timestamps
                            else:
                                ema = None  # Insufficient data for EMA
                                ema_timestamps = []

                            # Calculate Bollinger Bands
                            bb_period = min(20, buy_prices.size)  # Adjust the period as needed
                            buy_prices = buy_prices.astype('double')
                            if buy_prices.size >= bb_period:
                                upper, middle, lower = talib.BBANDS(buy_prices, timeperiod=bb_period)
                                bb_timestamps = buy_timestamps[-len(upper):]  # Match Bollinger Bands timestamps
                            else:
                                upper, middle, lower = None, None, None  # Insufficient data for Bollinger Bands
                                bb_timestamps = []

                            # Calculate MACD
                            if buy_prices.size >= 26:
                                macd, signal, _ = talib.MACD(buy_prices, fastperiod=12, slowperiod=26, signalperiod=9)
                                macd_timestamps = buy_timestamps[-len(macd):]  # Match MACD timestamps
                            else:
                                macd = None  # Insufficient data for MACD
                                signal = None
                                macd_timestamps = []

                            # Create a price history chart with all indicators
                            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(20, 15), gridspec_kw={'height_ratios': [3, 1, 1]})


                            # Plot the indicators on the price history chart
                            if sma is not None and len(sma) == len(sma_timestamps):
                                ax1.plot(sma_timestamps, sma, color='green', label=f'SMA ({sma_period}-period)')
                            if ema is not None and len(ema) == len(ema_timestamps):
                                ax1.plot(ema_timestamps, ema, color='yellow', label=f'EMA ({ema_period}-period)')
                            if upper is not None and lower is not None:
                                ax1.fill_between(bb_timestamps, upper, lower, color='brown', alpha=0.5, label='Bollinger Bands')

                            # Set the title and labels for the price history chart
                            ax1.set_title(f"Stock Chart for {stock_symbol}")
                            ax1.set_ylabel("Price")
                            ax1.grid(True)
                            ax1.legend()
                            ax1.tick_params(axis='x', rotation=45)
                            ax1.yaxis.set_major_formatter(ticker.FuncFormatter(price_formatter))

                            # Set y-axis limits on the price history chart
                            min_price = min(buy_prices.min(), sell_prices.min())
                            max_price = max(buy_prices.max(), sell_prices.max())
                            price_range = max_price - min_price
                            y_axis_margin = 0.001  # Add a margin to the y-axis limits
                            ax1.set_ylim(min_price - y_axis_margin * price_range, max_price + y_axis_margin * price_range)

                            # Set x-axis major locator and formatter based on time period
                            if "h" in time_period or "d" in time_period:
                                ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
                            else:
                                ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
                            ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))  # Format the date

                            # Plot the buy and sell transactions on the same price history chart
                            if price_debug:
                                ax1.plot(timestamps, prices, linestyle='-', color='green', label='Price')
                                ax1.scatter(buy_timestamps, buy_prices, marker='o', color='blue', label='Buy')
                                ax1.scatter(sell_timestamps, sell_prices, marker='o', color='red', label='Sell')

                            # Plot RSI on the separate RSI chart
                            if rsi is not None and len(rsi) == len(rsi_timestamps):
                                ax2.plot(rsi_timestamps, rsi, color='blue', label=f'RSI ({rsi_period}-period)')
                                ax2.set_title(f"RSI Chart for {stock_symbol}")
                                ax2.set_ylabel("RSI")
                                ax2.grid(True)
                                ax2.legend()

                                # Set x-axis limits to show only the beginning date and the end date
                                ax2.set_xlim(rsi_timestamps[0], rsi_timestamps[-1])

                                if "h" in time_period:
                                    ax2.xaxis.set_major_locator(mdates.DayLocator(interval=1))
                                elif "d" in time_period:
                                    ax2.xaxis.set_major_locator(mdates.DayLocator(interval=1))
                                else:
                                    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=1))

                                ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))  # Format the date
                                ax2.tick_params(axis='x', rotation=45)

                            # Plot MACD on the separate MACD chart
                            if macd is not None and len(macd) == len(macd_timestamps):
                                ax3.plot(macd_timestamps, macd, color='orange', label='MACD')
                                ax3.plot(macd_timestamps, signal, color='purple', label='Signal Line')
                                ax3.axhline(0, color='gray', linestyle='--', linewidth=0.8, label='Zero Line')
                                ax3.set_title(f"MACD Chart for {stock_symbol}")
                                ax3.set_ylabel("MACD")
                                ax3.grid(True)
                                ax3.legend()

                                # Set x-axis limits to show only the beginning date and the end date
                                ax3.set_xlim(macd_timestamps[0], macd_timestamps[-1])

                                if "h" in time_period:
                                    ax3.xaxis.set_major_locator(mdates.DayLocator(interval=1))
                                elif "d" in time_period:
                                    ax3.xaxis.set_major_locator(mdates.DayLocator(interval=1))
                                else:
                                    ax3.xaxis.set_major_locator(mdates.MonthLocator(interval=1))

                                ax3.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))  # Format the date
                                ax3.tick_params(axis='x', rotation=45)

                            # Set the background color of the entire chart to black
                            ax1.set_facecolor('black')
                            ax2.set_facecolor('black')
                            ax3.set_facecolor('black')

                            # Set text color to white
                            for ax in [ax1, ax2]:
                                ax.xaxis.label.set_color('lightgray')
                                ax.yaxis.label.set_color('lightgray')
                                ax.title.set_color('lightgray')
                                ax.tick_params(axis='x', colors='lightgray')
                                ax.tick_params(axis='y', colors='lightgray')

                            # Save the chart to a BytesIO object
                            buffer = io.BytesIO()
                            plt.savefig(buffer, format='png', facecolor='black')
                            buffer.seek(0)

                            # Send the chart as a Discord message
                            file = discord.File(buffer, filename='chart.png')
                            await ctx.send(file=file)

                            result = await lowest_price_order(self, ctx, "sell", stock_symbol)
                            if result:
                                order_price = result["price"]
                            else:
                                order_price = 0

                            current_price = await get_stock_price(self, ctx, stock_symbol)

                            # Create an embed with information
                            embed = discord.Embed(title=f"Stock Information for {stock_symbol}")
                            if stock_symbol.lower() == "roflstocks":
                                embed.add_field(name="Current Price", value=f"{current_price:,.11f}")
                                embed.add_field(name="Escrow Price", value=f"{order_price:,.11f}")
                                embed.add_field(name="Average Buy Price", value=f"{average_buy_price:,.11f}")
                                embed.add_field(name="Average Sell Price", value=f"{average_sell_price:,.11f}")
                            else:
                                embed.add_field(name="Current Price", value=f"{current_price:,.2f}")
                                embed.add_field(name="Escrow Price", value=f"{order_price:,.2f}")
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

                            # Set data structures to None to release memory
                            transactions = None
                            buy_prices = None
                            sell_prices = None
                            all_prices = None
                            timestamps = None
                            prices = None
                            actions = None
                            buy_timestamps = None
                            plt.close(fig)
                            plt.close('all')
                            fig.clf()

                            # Close the ledger database connection
                            ledger_conn.close()
                            await tax_command(self, ctx)
                            elapsed_time = timeit.default_timer() - self.tax_command_timer_start
                            self.tax_command_avg.append(elapsed_time)
                            avg_time = sum(self.tax_command_avg) / len(self.tax_command_avg)



                except sqlite3.Error as e:
                    await ctx.send(f"An error occurred: {str(e)}")





    @commands.command(name="transaction_counts", help="Shows the count of buy and sell transactions for a stock.")
    async def transaction_counts(self, ctx, stock_name: str):
        # Get transaction counts
        daily_buy_count, daily_sell_count = await count_transactions(stock_name, interval='daily')
        weekly_buy_count, weekly_sell_count = await count_transactions(stock_name, interval='weekly')
        monthly_buy_count, monthly_sell_count = await count_transactions(stock_name, interval='monthly')

        # Calculate total counts
        total_daily_transactions = daily_buy_count + daily_sell_count
        total_weekly_transactions = weekly_buy_count + weekly_sell_count
        total_monthly_transactions = monthly_buy_count + monthly_sell_count

        # Create an embed to display the counts
        embed = discord.Embed(
            title=f"Transaction Counts - {stock_name}",
            description=f"Transaction counts for {stock_name}",
            color=discord.Color.blue()
        )

        embed.add_field(name="Daily Transactions", value=f"Buys: {daily_buy_count}, Sells: {daily_sell_count}, Total: {total_daily_transactions}", inline=False)
        embed.add_field(name="Weekly Transactions", value=f"Buys: {weekly_buy_count}, Sells: {weekly_sell_count}, Total: {total_weekly_transactions}", inline=False)
        embed.add_field(name="Monthly Transactions", value=f"Buys: {monthly_buy_count}, Sells: {monthly_sell_count}, Total: {total_monthly_transactions}", inline=False)

        # Send the embed
        await ctx.send(embed=embed)


    @commands.command(name="cumulative_transaction_counts", help="Shows the cumulative count of buy and sell transactions for all stocks.")
    async def cumulative_transaction_counts(self, ctx):
        # Get cumulative transaction counts
        daily_buy_count, daily_sell_count = await count_all_transactions(interval='daily')
        weekly_buy_count, weekly_sell_count = await count_all_transactions(interval='weekly')
        monthly_buy_count, monthly_sell_count = await count_all_transactions(interval='monthly')

        # Calculate total counts
        total_daily_transactions = daily_buy_count + daily_sell_count
        total_weekly_transactions = weekly_buy_count + weekly_sell_count
        total_monthly_transactions = monthly_buy_count + monthly_sell_count

        # Create an embed to display the counts
        embed = discord.Embed(
            title="Cumulative Transaction Counts",
            description="Cumulative transaction counts for all stocks",
            color=discord.Color.blue()
        )

        embed.add_field(name="Daily Transactions", value=f"Buys: {daily_buy_count}, Sells: {daily_sell_count}, Total: {total_daily_transactions}", inline=False)
        embed.add_field(name="Weekly Transactions", value=f"Buys: {weekly_buy_count}, Sells: {weekly_sell_count}, Total: {total_weekly_transactions}", inline=False)
        embed.add_field(name="Monthly Transactions", value=f"Buys: {monthly_buy_count}, Sells: {monthly_sell_count}, Total: {total_monthly_transactions}", inline=False)

        # Send the embed
        await ctx.send(embed=embed)


    @commands.command(name='reset_levels', help='Reset all user levels, experience, and RPG stats')
    @is_allowed_user(930513222820331590)
    async def reset_levels(self, ctx):
        try:
            cursor = self.conn.cursor()

            # Delete all records from the users_level table
            cursor.execute("DELETE FROM users_level")

            # Delete all records from the users_rpg_stats table
            cursor.execute("DELETE FROM users_rpg_stats")

            self.conn.commit()

            await ctx.send("All user levels, experience, and RPG stats have been reset.")
        except sqlite3.Error as e:
            print(f"Error resetting levels and RPG stats: {e}")
            await ctx.send("An error occurred while resetting user levels, experience, and RPG stats.")



    @commands.command(name="tax_refund", help="Distribute this month's 10% of received funds to all users in the server.")
    @is_allowed_user(930513222820331590)
    async def tax_refund(self, ctx, debug: bool = False):
        try:
            # Connect to the SQLite databases
            ledger_conn = sqlite3.connect("p3ledger.db")
            P3conn = sqlite3.connect("P3addr.db")
            currency_conn = sqlite3.connect("currency_system.db")
            currency_cursor = currency_conn.cursor()

            # Retrieve transfer transactions for the current month where the bot is the receiver
            current_month = datetime.now().month
            ledger_cursor = ledger_conn.cursor()
            ledger_cursor.execute("SELECT sender_id, amount FROM transfer_transactions WHERE receiver_id = ? AND strftime('%m', timestamp) = ?;", (str(self.bot.user.id), str(current_month)))
            received_tokens = ledger_cursor.fetchall()

            # Calculate total amount for the current month
            total_amount = sum(received_token[1] for received_token in received_tokens)

            # Calculate 10% of the total amount
            refund_amount = total_amount * 0.05

            # Query user_addresses to get the list of users
            P3cursor = P3conn.cursor()
            P3cursor.execute("SELECT user_id, p3_address FROM user_addresses")
            users = P3cursor.fetchall()

            # Calculate refund amount per user
            num_recipients = len(users)
            individual_refund = refund_amount / num_recipients

            # Distribute the refund to all users (except the bot and other bots)
            for user_id, p3_address in users:
                try:
                    recipient_user = await self.bot.fetch_user(int(user_id))
                    if recipient_user and not recipient_user.bot and str(user_id) != str(self.bot.user.id):
                        # Check if the user is staking_qse_genesis and apply an extra 25%
                        is_staking_qse_genesis = self.is_staking_qse_genesis(user_id)
                        if is_staking_qse_genesis:
                            individual_refund *= 1.25  # Apply an extra 25%

                        # Convert individual_refund to Decimal before adding
                        individual_refund_decimal = Decimal(str(individual_refund))

                        # Print debug information
                        if debug:
                            print(f"User ID: {user_id}, P3 Address: {p3_address}, Refund Amount: {individual_refund_decimal:,.2f}")
                        else:
                            # Add the refund amount to the recipient's balance
                            await update_user_balance(currency_conn, int(user_id), get_user_balance(currency_conn, int(user_id)) + individual_refund_decimal)

                            # Log the tax refund
                            await log_transfer(self, ledger_conn, ctx, "P3 Bot", ctx.author.name, int(user_id), individual_refund_decimal)

                except discord.NotFound:
                    print(f"User not found with ID: {user_id}")

            # Close the database connections
            ledger_conn.close()
            P3conn.close()
            currency_conn.close()

            # Send a confirmation message
            await ctx.send(f"Gas refund distributed to {num_recipients} users.")

        except Exception as e:
            print(f"Error in Gas_refund: {e}")
            await ctx.send("An error occurred while processing the Gas refund.")


    @commands.command(name="CTR", aliases=["calculate_tax_refund"])
    async def calculate_tax_refund(self, ctx, option: str = "current"):  # Add the 'self' parameter
        self.tax_command_timer_start = timeit.default_timer()
        try:
            # Connect to the SQLite databases
            P3conn = sqlite3.connect("P3addr.db")
            ledger_conn = sqlite3.connect("p3ledger.db")

            # Calculate the start date based on the option
            if option.lower() == "last":
                current_month_start = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                last_month_start = current_month_start - timedelta(days=1)
                last_month_start = last_month_start.replace(day=1)
                start_date = last_month_start
                title_prefix = "Last Month"
            elif option.lower() == "total":
                start_date = datetime.min
                title_prefix = "Total History"
            else:
                start_date = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                title_prefix = "This Month"

            # Retrieve transfer transactions where the bot is the receiver for the selected period
            ledger_cursor = ledger_conn.cursor()
            ledger_cursor.execute("SELECT sender_id, amount, timestamp FROM transfer_transactions WHERE receiver_id = ? AND timestamp >= ?;", (str(self.bot.user.id), start_date))
            received_tokens = ledger_cursor.fetchall()

            # Calculate total amount
            total_amount = sum(received_token[1] for received_token in received_tokens)

            # Calculate 10% of the total amount
            refund_amount = total_amount * 0.05

            # Query user_addresses to get the list of users
            P3cursor = P3conn.cursor()
            P3cursor.execute("SELECT user_id, p3_address FROM user_addresses")
            users = P3cursor.fetchall()

            # Calculate number of recipients
            num_recipients = len(users)

            # Calculate refund amount per user
            individual_refund = refund_amount / num_recipients

            # Calculate tax collected in the last 1, 5, 30, 6, 12, and 24 hours
            current_time = datetime.now()
            tax_last_1_hour = sum(received_token[1] for received_token in received_tokens if current_time - timedelta(hours=1) <= datetime.strptime(received_token[2], "%Y-%m-%d %H:%M:%S") <= current_time)
            tax_last_5_minutes = sum(received_token[1] for received_token in received_tokens if current_time - timedelta(minutes=5) <= datetime.strptime(received_token[2], "%Y-%m-%d %H:%M:%S") <= current_time)
            tax_last_30_minutes = sum(received_token[1] for received_token in received_tokens if current_time - timedelta(minutes=30) <= datetime.strptime(received_token[2], "%Y-%m-%d %H:%M:%S") <= current_time)
            tax_last_6_hours = sum(received_token[1] for received_token in received_tokens if current_time - timedelta(hours=6) <= datetime.strptime(received_token[2], "%Y-%m-%d %H:%M:%S") <= current_time)
            tax_last_12_hours = sum(received_token[1] for received_token in received_tokens if current_time - timedelta(hours=12) <= datetime.strptime(received_token[2], "%Y-%m-%d %H:%M:%S") <= current_time)
            tax_last_24_hours = sum(received_token[1] for received_token in received_tokens if current_time - timedelta(days=1) <= datetime.strptime(received_token[2], "%Y-%m-%d %H:%M:%S") <= current_time)

            # Close the database connections
            P3conn.close()
            ledger_conn.close()

            # Create an embed with emojis
            embed = discord.Embed(title=f"💸 {title_prefix} Gas Refund Calculation 💸", color=0x42F56C)
            embed.add_field(name="💰 Total amount received", value=f"{total_amount:,.2f} tokens", inline=False)
            embed.add_field(name="💸 5% of the total amount", value=f"{refund_amount:,.2f} tokens", inline=False)
            embed.add_field(name="👥 Number of recipients", value=f"{num_recipients}", inline=False)
            embed.add_field(name="💳 Gas refund amount per user", value=f"{individual_refund:,.2f} tokens", inline=False)
#            embed.add_field(name="🕕 Gas collected in the last 5 minutes", value=f"{tax_last_5_minutes:,.2f} tokens", inline=False)
#            embed.add_field(name="🕤 Gas collected in the last 30 minutes", value=f"{tax_last_30_minutes:,.2f} tokens", inline=False)
#            embed.add_field(name="🕒 Gas collected in the last 1 hour", value=f"{tax_last_1_hour:,.2f} tokens", inline=False)
#            embed.add_field(name="🕕 Gas collected in the last 6 hours", value=f"{tax_last_6_hours:,.2f} tokens", inline=False)
#            embed.add_field(name="🕛 Gas collected in the last 12 hours", value=f"{tax_last_12_hours:,.2f} tokens", inline=False)
#            embed.add_field(name="🕧 Gas collected in the last 24 hours", value=f"{tax_last_24_hours:,.2f} tokens", inline=False)

            # Send the embed to the user
            await ctx.send(embed=embed)
            await tax_command(self, ctx)
            elapsed_time = timeit.default_timer() - self.tax_command_timer_start
            self.tax_command_avg.append(elapsed_time)
            avg_time = sum(self.tax_command_avg) / len(self.tax_command_avg)


        except Exception as e:
            print(f"Error in calculate_Gas_refund: {e}")
            await ctx.send("An error occurred while calculating the Gas refund.")





    @commands.command(name="etf_chart")
    async def etf_chart(self, ctx, etf_symbol, time_period=None, rsi_period=None, sma_period=None, ema_period=None):
        self.tax_command_timer_start = timeit.default_timer()
        try:
            locale.setlocale(locale.LC_ALL, '')
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
                    all_prices = []

                    for timestamp_str, action, price_str in transactions:
                        formatted_price = str(price_str).replace(",", "")
                        price = float(formatted_price)
                        datetime_obj = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")  # Parse timestamp string
                        all_prices.append((datetime_obj, price, action))

                        if "Buy" in action:
                            buy_prices.append((datetime_obj, price))
                        elif "Sell" in action:
                            sell_prices.append((datetime_obj, price))


                    # Sort all prices by timestamp
                    all_prices.sort(key=lambda x: x[0])

                    # Extract datetime objects and prices for buy and sell transactions
                    timestamps = [entry[0] for entry in all_prices]
                    prices = [entry[1] for entry in all_prices]
                    actions = [entry[2] for entry in all_prices]
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
                    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(20, 15), gridspec_kw={'height_ratios': [3, 1]})

                    # Calculate average line
                    average_prices = [(t, (b + s) / 2) for t, b, s in zip(buy_timestamps, buy_prices, sell_prices)]
                    average_timestamps, average_prices = zip(*average_prices)

                    # Plot the average line on the price history chart
#                    ax1.plot(buy_timestamps, buy_prices, linestyle='--', color='lightgray', label='Price')
                    if sma is not None and len(sma) == len(sma_timestamps):
                        ax1.plot(sma_timestamps, sma, color='green', label=f'SMA ({sma_period}-period)')
                    if ema is not None and len(ema) == len(ema_timestamps):
                        ax1.plot(ema_timestamps, ema, color='yellow', label=f'EMA ({ema_period}-period)')
                    if upper is not None and lower is not None:
                        ax1.fill_between(bb_timestamps, upper, lower, color='brown', alpha=0.5, label='Bollinger Bands')

                    ax1.set_title(f"ETF Chart for {etf_symbol}")
                    ax1.set_ylabel("Price")
                    ax1.yaxis.set_major_formatter(ticker.FuncFormatter(price_formatter))
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
                    # Plot the buy and sell transactions on the same price history chart
                    ax1.plot(timestamps, prices, linestyle='-', color='green', label='Price')
                    ax1.scatter(buy_timestamps, buy_prices, marker='o', color='blue', label='Buy')
                    ax1.scatter(sell_timestamps, sell_prices, marker='o', color='red', label='Sell')


                    # Plot RSI on the separate RSI chart
                    if rsi is not None and len(rsi) == len(rsi_timestamps):
                        ax2.plot(rsi_timestamps, rsi, color='blue', label=f'RSI ({rsi_period}-period)')
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


                    # Set the background color of the entire chart to black
                    ax1.set_facecolor('black')
                    ax2.set_facecolor('black')

                    # Set text color to white
                    for ax in [ax1, ax2]:
                        ax.xaxis.label.set_color('lightgray')
                        ax.yaxis.label.set_color('lightgray')
                        ax.title.set_color('lightgray')
                        ax.tick_params(axis='x', colors='lightgray')
                        ax.tick_params(axis='y', colors='lightgray')

                    # Save the chart to a BytesIO object
                    buffer = io.BytesIO()
                    plt.savefig(buffer, format='png', facecolor='black')
                    buffer.seek(0)


                    # Send the chart as a Discord message
                    file = discord.File(buffer, filename='etf_chart.png')
                    await ctx.send(file=file)

                    # Create an embed with information
                    embed = discord.Embed(title=f"ETF Information for {etf_symbol}")
                    embed.add_field(name="Current Price", value=locale.currency(current_price, grouping=True))
                    embed.add_field(name="Average Buy Price", value=locale.currency(average_buy_price, grouping=True))
                    embed.add_field(name="Average Sell Price", value=locale.currency(average_sell_price, grouping=True))
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
                    await tax_command(self, ctx)
                    elapsed_time = timeit.default_timer() - self.tax_command_timer_start
                    self.tax_command_avg.append(elapsed_time)
                    avg_time = sum(self.tax_command_avg) / len(self.tax_command_avg)


        except sqlite3.Error as e:
            await ctx.send(f"An error occurred: {str(e)}")


# Stock Market



    async def check_market(self, ctx):
#        if self.market_breaker != True:
#            return
        etf_id = 6
        etf_value = await get_etf_value(self.conn, etf_id)

        if self.last_market_value != 0:
            percentage_change = ((etf_value - self.last_market_value) / self.last_market_value) * 100
            print(f"Market Monitor: {percentage_change}%")

            if self.market_circuit_breaker == True:
                if abs(percentage_change) >= self.market_limit:
                    # Halt trading for 30 minutes
                    self.is_halted = True
                    embed = discord.Embed(description=f"Trading Halted due to a significant market change. Waiting for {(self.market_timeout / 60)} minutes.", color=discord.Color.red())
                    await ctx.send(embed=embed)

                    self.market_halts += 1
                    self.last_market_value = 0.0

                    await asyncio.sleep(self.market_timeout)  # Sleep for 30 minutes

                    self.is_halted = False
                    embed = discord.Embed(description="Trading Resumed after 5 minutes.", color=discord.Color.green())
                    await ctx.send(embed=embed)

        self.last_market_value = etf_value


    async def check_stock_change(self, ctx, symbol: str):
        current_price = await get_stock_price(self, ctx, symbol)

        # Check if the symbol is already in self.stock_monitor
        if symbol not in self.stock_monitor:
            self.stock_monitor[symbol] = []  # Initialize an empty list for the symbol

        # Append the current price
        self.stock_monitor[symbol].append(current_price)

        # Check if the list has more than 5 entries, and if so, remove the oldest entries
        if len(self.stock_monitor[symbol]) > 5:
            self.stock_monitor[symbol] = self.stock_monitor[symbol][-5:]

            # Compare the oldest and latest prices
            oldest_price = self.stock_monitor[symbol][0]
            latest_price = self.stock_monitor[symbol][-1]
            percentage_change = ((latest_price - oldest_price) / oldest_price) * 100

            # Print a statement if the price change is more than 100%
            if self.stock_circuit_breaker == True:
                if abs(percentage_change) > self.stock_limit:
                    print(f"Trading Halted for {symbol} due to significant market change")
                    embed = discord.Embed(description=f"Trading Halted due to a significant market change for {symbol}. Waiting for {(self.stock_timeout / 60)} minutes.", color=discord.Color.red())
                    await ctx.send(embed=embed)
                    self.stock_halts += 1
                    self.not_trading.append(symbol.lower())
                    await asyncio.sleep(self.stock_timeout)
                    self.stock_monitor.pop(symbol)
                    self.not_trading.remove(symbol.lower())
                    return


    @commands.command(name="circuit_stats", aliases=["qsec_config", "qsec_stats"])
    async def circuit_stats(self, ctx):
        if self.is_halted:
            color=discord.Color.red()
        else:
            color=discord.Color.blue()
        embed = discord.Embed(title=f"QSec Platform Config/Stats for QSE Blockchain", color=discord.Color.blue())
        embed.add_field(name="Market Halted:", value=f"{self.is_halted}", inline=False)
        embed.add_field(name="Assets Halted", value=f"{self.not_trading}", inline=False)
        embed.add_field(name="Assets in Debug", value=f"{self.maintenance}", inline=False)
        embed.add_field(name="Market Circuit Breaker:", value=f"{self.market_circuit_breaker}", inline=False)
        embed.add_field(name="Market Circuit Limit:", value=f"{self.market_limit}%", inline=False)
        embed.add_field(name="Market Timeout:", value=f"{(self.market_timeout / 60):,.0f} minutes", inline=False)
        embed.add_field(name="Stock Circuit Breaker:", value=f"{self.stock_circuit_breaker}", inline=False)
        embed.add_field(name="Stock Circuit Limit:", value=f"{self.stock_limit}%", inline=False)
        embed.add_field(name="Stock Timeout:", value=f"{(self.stock_timeout / 60):,.0f} minutes", inline=False)
        embed.add_field(name="Current Pool Size:", value=f"{len(self.transaction_pool)}", inline=False)
        embed.add_field(name="Highest Pool Size:", value=f"{self.total_pool}", inline=False)
        embed.add_field(name="Market Halts since last Reload:", value=f"{self.market_halts}", inline=False)
        embed.add_field(name="Stock Halts since last Reload:", value=f"{self.stock_halts}", inline=False)


        await ctx.send(embed=embed)

    @commands.command(name="circuit_breaker")
    @is_allowed_user(930513222820331590, PBot)
    async def circuit_breaker(self, ctx, circuit: str, halt: bool):
        if circuit.lower() == "market":
            if halt == True:
                self.market_circuit_breaker = True
                embed = discord.Embed(description=f"{circuit} circuit breaker on", color=discord.Color.green())
                await ctx.send(embed=embed)
            else:
                self.market_circuit_breaker = False
                embed = discord.Embed(description=f"{circuit} circuit breaker off", color=discord.Color.red())
                await ctx.send(embed=embed)
        elif circuit.lower() == "stock":
            if halt == True:
                self.stock_circuit_breaker = True
                embed = discord.Embed(description=f"{circuit} circuit breaker on", color=discord.Color.green())
                await ctx.send(embed=embed)
            else:
                self.stock_circuit_breaker = False
                embed = discord.Embed(description=f"{circuit} circuit breaker off", color=discord.Color.red())
                await ctx.send(embed=embed)
        else:
            await ctx.send("Wrong Syntax")


    @commands.command(name="halt_trading")
    @is_allowed_user(930513222820331590, PBot)
    async def halt_trading(self, ctx, halt: bool):
        if halt:
            self.is_halted = True
            embed = discord.Embed(description="Trading Halted", color=discord.Color.red())
            await ctx.send(embed=embed)
        else:
            self.is_halted = False
            embed = discord.Embed(description="Trading Resumed", color=discord.Color.green())
            await ctx.send(embed=embed)


    @commands.command(name="halt_trading_order")
    @is_allowed_user(930513222820331590, PBot)
    async def halt_trading_order(self, ctx, halt: bool):
        if halt:
            self.is_halted_order = True
            embed = discord.Embed(description="Trading Halted", color=discord.Color.red())
            await ctx.send(embed=embed)
        else:
            self.is_halted_order = False
            embed = discord.Embed(description="Trading Resumed", color=discord.Color.green())
            await ctx.send(embed=embed)

    @commands.command(name="halt_trading_stock")
    @is_allowed_user(930513222820331590, PBot)
    async def halt_trading_stock(self, ctx, stock:str, halt: bool):
        if halt:
            self.not_trading.append(stock.lower())
            embed = discord.Embed(description=f"Trading Halted for {stock}", color=discord.Color.red())
            await ctx.send(embed=embed)
        else:
            self.not_trading.remove(stock.lower())
            if stock in self.stock_monitor:
                self.stock_monitor.pop(stock)
            embed = discord.Embed(description=f"Trading Resumed for {stock}", color=discord.Color.green())
            await ctx.send(embed=embed)

    @commands.command(name="add_ipo")
    @is_allowed_user(930513222820331590, PBot)
    async def add_ipo(self, ctx, stock:str, ipo: bool):
        if ipo:
            self.ipo_stocks.append(stock.lower())
            embed = discord.Embed(description=f"{stock} added to IPO", color=discord.Color.green())
            await ctx.send(embed=embed)
        else:
            self.ipo_stocks.remove(stock.lower())
            embed = discord.Embed(description=f"{stock} removed from IPO", color=discord.Color.red())
            await ctx.send(embed=embed)


    async def process_transaction(self, ctx, type, symbol, amount, action):
        try:
            async with self.db_semaphore:
                async with self.transaction_lock:
                    if action == "buy":
                        if type.lower() == "order":
                            print("Order")
                            await self.transact_order2(ctx, str(symbol), int(amount))
                        elif type.lower() == "stock":
                            self.buy_timer_start = timeit.default_timer()
                            await self.buy_stock(ctx, symbol, amount)
                        elif type.lower() == "item":
                            await ctx.send("Item purchases are disabled ahead of P3 resource update")
                            return
                            self.buy_item_timer_start = timeit.default_timer()
                            await self.buy_item(ctx, symbol, amount)
                        elif type.lower() == "etf":
                            if not symbol.isdigit():
                                await ctx.send("Invalid ETF symbol. ETF symbol must be an integer.")
                                return
                            for i in self.etfs:
                                if symbol not in self.etfs:
                                    await ctx.send(f"ETF {symbol} does not exist")
                                    return
                            self.buy_etf_timer_start = timeit.default_timer()
                            await self.buy_etf(ctx, symbol, amount)
                        else:
                            await ctx.send(f"Transaction Error: {symbol} must be Stock, Item, or ETF")
                    else:
                        if type.lower() == "stock":
                            self.sell_timer_start = timeit.default_timer()
                            if symbol.lower() not in self.ipo_stocks:
                                await self.sell_stock(ctx, symbol, amount)
                            else:
                                embed = discord.Embed(description=f"{symbol} is in IPO and cannot be sold during the IPO Phase", color=discord.Color.red())
                                await ctx.send(embed=embed)
                                return
                        elif type.lower() == "item":
                            self.sell_item_timer_start = timeit.default_timer()
                            await self.sell_item(ctx, symbol, amount)
                        elif type.lower() == "etf":
                            if not symbol.isdigit():
                                embed = discord.Embed(description=f"Invalid ETF symbol. ETF symbol must be an integer.", color=discord.Color.red())
                                await ctx.send(embed=embed)
                                return
                            self.sell_etf_timer_start = timeit.default_timer()
                            await self.sell_etf(ctx, symbol, amount)
                        else:
                            embed = discord.Embed(description=f"Transaction Error: {symbol} must be Stock, Item, or ETF", color=discord.Color.red())
                            await ctx.send(embed=embed)
                            return
        except Exception as e:
            embed = discord.Embed(description=f"Transaction failed: for {symbol}\nTry again shortly...", color=discord.Color.red())
            await ctx.send(embed=embed)
            if str(e) != "404 Not Found (error code: 10008): Unknown Message":
                #self.skipped_transactions.append((ctx, type, symbol, amount, action))
                print(f"Transaction failed: {str(e)}")
        except sqlite3.Error as e:
            # Check if the error message contains the specific error
            if "Python int too large to convert to SQLite INTEGER" in str(e):
                embed = discord.Embed(description="Transaction Failed: Try a smaller quantity", color=discord.Color.red())
                await ctx.send(embed=embed)
                return

    async def process_transactions(self, ctx):
        print("Grabbing Pool")
        while self.transaction_pool or self.skipped_transactions:
            print("Aquiring Lock")
            async with self.transaction_lock:
                print("Beginning Transaction")
                if self.skipped_transactions:
                    # Add skipped transactions back to the pool
                    self.transaction_pool.extend(self.skipped_transactions)
                    self.skipped_transactions = []

                if not self.transaction_pool:
                    break

                print(f"Pool Size: {len(self.transaction_pool)}")
                print(f"Skipped Pool Size: {len(self.skipped_transactions)}")
                if len(self.transaction_pool) > self.total_pool:
                    pool_difference = (len(self.transaction_pool) - self.total_pool)
                    self.total_pool += pool_difference
                ctx, type, symbol, amount, action = self.transaction_pool.pop(0)
            await self.process_transaction(ctx, type, symbol, amount, action)
            await self.check_market(ctx)







    @commands.command(name="order")
    async def place_order(self, ctx, order_type: str, symbol: str, price: str, quantity: str):
        price = int(price.replace(",", ""))
        quantity = int(quantity.replace(",", ""))
        mv = await get_etf_value(self.conn, 6)
        await add_mv_metric(self, ctx, mv)
        await add_reserve_metric(self, ctx)
        mv_avg = await calculate_average_mv(self)
        print(f"MV Avg: {mv_avg:,.2f}")
        if symbol.lower() in self.ipo_stocks and ctx.author.id != 607050637292601354:
            await ctx.send(f"{symbol} currently in IPO, cannot place orders")
            return
        if int(price) > 250000000:
            await ctx.send("Price cannot be over 250,000,000 QSE per Share")
            return
        if order_type.lower() == "buy":
            user_owned = self.get_user_stock_amount(ctx.author.id, symbol)
            result = await get_supply_stats(self, ctx, symbol)
            reserve, total, locked, escrow, market, circulating = result
            if (user_owned + int(quantity)) > (total * 0.51):
                embed = discord.Embed(description=f"{ctx.author.mention}, you cannot own more than 51% of the total supply of {stock_name} stocks.\nAvailable: {market:,}\nTotal: {total:,}\nYour Shares: {user_owned:,}", color=discord.Color.red())
                await ctx.send(embed=embed)
                return
            else:
                user_owned = self.get_user_stock_amount(ctx.author.id, symbol)
                result = await get_supply_stats(self, ctx, symbol)
                reserve, total, locked, escrow, market, circulating = result
                escrow_user_shares = await get_total_shares_user_order(self, ctx.author.id, symbol)
                if (user_owned + int(quantity) + escrow_user_shares) > (total * 0.51):
                    embed = discord.Embed(description=f"{ctx.author.mention}, you cannot own more than 51% of the total supply of {stock_name} stocks.\nAvailable: {market:,}\nTotal: {total:,}\nYour Shares + escrow: {user_owned:,} + {escrow_user_shares:,}", color=discord.Color.red())
                    await ctx.send(embed=embed)
                else:
                    await self.add_limit_order_command(ctx, symbol, order_type, price, quantity)
        elif order_type.lower() == "sell":
            user_owned = self.get_user_stock_amount(ctx.author.id, symbol)
            if user_owned < int(quantity):
                await ctx.send(f"Not enough {symbol} in stash")
                return
            else:
                await self.add_limit_order_command(ctx, symbol, order_type, price, quantity)
                await add_experience(self, self.conn, ctx.author.id, 2.5, ctx)
        else:
            await ctx.send("Command syntax: !order order_type, symbol, price, quantity")




    @commands.command(name="add_limit_order", aliases=["place_order"], help="Add a limit order.")
    async def add_limit_order_command(self, ctx, symbol: str, order_type: str, price: float, quantity: int):
        max_share_limit = 500_000_000_000_000_000_000
        max_value_limit = 500_000_000_000_000_000_000_000

        order_value = int(quantity) * int(price)

        no_order = ["p3:stable", "roflstocks"]
        if symbol.lower() in no_order:
            await ctx.send(f"Orders not allowed for {symbol}")
            return

        if order_type.lower() == "buy":
            lowest_sell = await lowest_price_order(self, ctx, "sell", symbol)
            highest_buy = await highest_price_order(self, ctx, "buy", symbol)

            if lowest_sell and int(price) > int(lowest_sell["price"]):
                await ctx.send(f"Buy order must be lower than {lowest_sell['price']:,.0f} QSE per Share")
                return

            if highest_buy and int(price) > int(highest_buy["price"]):
                await ctx.send(f"Buy order must be lower than {highest_buy['price']:,.0f} QSE per Share")
                return

        if order_type.lower() == "sell":
            highest = await highest_price_order(self, ctx, "buy", symbol)
            lowest = await lowest_price_order(self, ctx, "sell", symbol)
            if highest and lowest:
                if int(highest["price"]) > int(lowest["price"]):
                    await ctx.send(f"Sell order must be higher than {highest['price']:,.0f} QSE per Share")
                    return
            if highest:
                if int(price) < int(highest["price"]):
                    await ctx.send(f"Sell order must be higher than {highest['price']:,.0f} QSE per Share")
                    return

        user_id = ctx.author.id

        # Determine if the order needs to be broken into chunks
        if int(quantity) > max_share_limit or order_value >= max_value_limit:
            # Use larger chunks without exceeding the limits
            max_chunk_size = min(max_share_limit, max_value_limit // price)
            chunks = int((quantity + max_chunk_size - 1) / max_chunk_size)  # Fix: Convert to int
            chunk_quantity = int(quantity) // chunks
        else:
            chunks = 1
            chunk_quantity = int(quantity)

        orders = []
        for _ in range(chunks):
            orders.append((self, ctx, user_id, symbol, order_type, int(price), chunk_quantity))
        await add_limit_orders(self, ctx, orders, True)



    @commands.command(name="add_limit_order_bot", aliases=["place_order_bot"], help="Add a limit order.")
    async def add_limit_order_bot(self, ctx, symbol: str, price: float, quantity: int):
        try:
            await add_limit_order(self, ctx, symbol, price, quantity)
            await ctx.send("Order listed")
        except:
            await ctx.send("Failed to list order")




    @commands.command(name="mint")
    @is_allowed_user(930513222820331590, PBot)
    async def mint_qse(self, ctx, amount):
        amount = int(amount)
        old_balance = get_user_balance(self.conn, PBot)
        await mint_to_reserve(self, ctx, amount)
        new_balance = get_user_balance(self.conn, PBot)
        mv = await get_etf_value(self.conn, 6)
        await add_mv_metric(self, ctx, mv)
        await add_reserve_metric(self, ctx)
        await ctx.send(f"Minted {amount:,.0f} QSE to the Reserve\n\nPrevious Funds: {old_balance:,.2f}\nNew Funds: {new_balance:,.2f}")




    @commands.command(name="remove_order", help="Remove a limit order by order_id.")
    @is_allowed_user(930513222820331590, PBot)
    async def remove_limit_order_command(self, ctx, order_id: int):
        await remove_limit_order(self, ctx, order_id)

    @commands.command(name="read_limit_orders", help="Read all limit orders.")
    @is_allowed_user(930513222820331590, PBot)
    async def read_limit_orders_command(self, ctx):
        await read_limit_orders(self, ctx)


    @commands.command(name="lowest_price_order", help="Show the lowest price order based on type and symbol.")
    async def lowest_price_order_command(self, ctx, order_type: str, symbol: str, highlow: str):
        if highlow == "low":
            result = await lowest_price_order(self, ctx, order_type, symbol)
        elif highlow == "high":
            result = await highest_price_order(self, ctx, order_type, symbol)
        else:
            return
        if result:
            print(f"Order ID: {result['order_id']}")
            print(f"User ID: {result['user_id']}")
            print(f"Symbol: {result['symbol']}")
            print(f"Order Type: {result['order_type']}")
            print(f"Price: {result['price']}")
            print(f"Quantity: {result['quantity']}")
            print(f"Created At: {result['created_at']}")
        else:
            print(f"No {order_type} order for {symbol}")

    @commands.command(name="buy", help="Buy Stocks, ETFs, and Items")
    @is_allowed_server(1161678765894664323, 1087147399371292732)
    async def buy(self, ctx, type: str, symbol = None, amount = None):
        if amount != None:
            if isinstance(amount, str):
                amount = int(amount.replace(",", ""))
        userid = ctx.author.id


        if check_current_hp(userid) == 0:
            embed = discord.Embed(description="Your HP is zero, Please Heal", color=discord.Color.red())
            await ctx.send(embed=embed)
            return

        if type.lower() == "option":
            if amount == "None":
                await self.buy_option(ctx, symbol)
                return
            else:
                if amount > 100_000:
                    chunks = (amount // 100_000)  # Use integer division to get whole chunks
                    chunk_amount = amount // chunks  # Calculate the amount per chunk
                    remaining_amount = amount % chunks  # Calculate the remaining amount
                    for _ in range(chunks):
                        await self.buy_option(ctx, symbol, chunk_amount)
                        if remaining_amount > 0:
                            await self.buy_option(ctx, symbol, remaining_amount)
                else:
                    await self.buy_option(ctx, symbol, amount)
                    return


        if type.lower() == "order":
            if int(amount) > 500000000000000:
                embed = discord.Embed(description="Can only purchase 500,000,000,000,000 at a time", color=discord.Color.red())
                await ctx.send(embed=embed)
                return
            if self.is_halted_order:
                embed = discord.Embed(description="Order Trading Halted", color=discord.Color.red())
                await ctx.send(embed=embed)
                return
            else:
                try:
                    user_id = ctx.author.id
                    p3addr = get_p3_address(self.P3addrConn, user_id)



                    print(f"\n\nDebug Buy Wrapper\nType: {type}\nAsset: {symbol}\nQuantity: {amount:,.0f}\nUserID: {user_id}\nP3 Address: {p3addr}\n\n")
                    self.transaction_pool.append((ctx, type, symbol, amount, "buy"))
                    if len(self.transaction_pool) != 0:
                        print("Processing Transactions")
                        await self.process_transactions(ctx)
                        await autoliquidate(self, ctx)
                    return
                except discord.errors.GatewayNotFound as e:
                    print(f"Gateway not found warning: {e}")
                    await ctx.send("Warning: Gateway not found. canceling order")
                    return
                except sqlite3.Error as e:
                    # Check if the error message contains the specific error
                    if "Python int too large to convert to SQLite INTEGER" in str(e):
                        embed = discord.Embed(description="Transaction Failed: Try a smaller quantity", color=discord.Color.red())
                        await ctx.send(embed=embed)
                        return

        if type.lower() == "stock":
            current_price = await get_stock_price(self, ctx, symbol)
            value = int(current_price) * int(amount)
            if int(value) > 10_000_000_000_000_000_000_000:
                embed = discord.Embed(description="Can only purchase 10,000,000,000,000,000,000,000 of value at a time", color=discord.Color.red())
                await ctx.send(embed=embed)
                return
            order_price = await lowest_price_order(self, ctx, "sell", symbol)
            total_supply, available = await get_supply_info(self, ctx, symbol)
            escrow_supply = await get_total_shares_in_orders(self, symbol)
            reserve_supply = self.get_user_stock_amount(PBot, symbol)
            reserve_supply = reserve_supply - escrow_supply

            if reserve_supply == 0:
                embed = discord.Embed(description="Zero market supply...attempting to find an open sell order", color=discord.Color.blue())
                await ctx.send(embed=embed)


                if self.is_halted_order:
                    embed = discord.Embed(description="Zero market supply and order book trading halted", color=discord.Color.red())
                    await ctx.send(embed=embed)
                    return
                else:
                    if order_price:
                        self.transaction_pool.append((ctx, type, symbol, amount, "buy"))
                        if len(self.transaction_pool) != 0:
                            print("Processing Transactions")
                            await self.process_transactions(ctx)
                            await autoliquidate(self, ctx)
                            return

                    else:
                        embed = discord.Embed(description="Zero market supply and couldn't find an order", color=discord.Color.red())
                        await ctx.send(embed=embed)
                        return

            if reserve_supply < amount:
                embed = discord.Embed(description=f"Only {reserve_supply} shares", color=discord.Color.blue())
                await ctx.send(embed=embed)
                return


        if symbol.lower() in self.ipo_stocks and current_price > self.ipo_price_limit:
            self.ipo_stocks.remove(symbol.lower())
            embed = discord.Embed(description=f"{symbol} has left IPO @everyone", color=discord.Color.green())
            await ctx.send(embed=embed)
            return
        if len(self.transaction_pool) > 0:
            embed = discord.Embed(description=f"Transaction Pending, you have {len(self.transaction_pool)} ahead of you. ", color=discord.Color.yellow())
            await ctx.send(embed=embed)


        if await self.is_trading_halted() == True:
            embed = discord.Embed(description="Trading Temporarily Halted", color=discord.Color.red())
            await ctx.send(embed=embed)
            return
        if await self.is_trading_halted_stock(symbol) == True:
            embed = discord.Embed(description=f"Trading Temporarily Halted for {symbol}", color=discord.Color.red())
            await ctx.send(embed=embed)
            return
        await check_store_addr(self, ctx)
        if type.lower() == "stock":
            await self.check_stock_change(ctx, symbol)
        user_id = ctx.author.id
        p3addr = get_p3_address(self.P3addrConn, user_id)

        try:

            print(f"\n\nDebug Buy Wrapper\nType: {type}\nAsset: {symbol}\nQuantity: {amount:,.0f}\nUserID: {user_id}\nP3 Address: {p3addr}\n\n")
            async with self.transaction_lock:
                self.transaction_pool.append((ctx, type, symbol, amount, "buy"))
            if len(self.transaction_pool) != 0:
                print("Processing Transactions")
                await self.process_transactions(ctx)
                await autoliquidate(self, ctx)
                mv = await get_etf_value(self.conn, 6)
                await add_mv_metric(self, ctx, mv)
                await add_reserve_metric(self, ctx)
                mv_avg = await calculate_average_mv(self)
                print(f"MV Avg: {mv_avg:,.2f}")
        except ValueError:
            await ctx.send("Invalid amount. Please provide a valid integer.")

    @commands.command(name="sell", help="Buy Stocks, ETFs, and Items")
    @is_allowed_server(1161678765894664323, 1087147399371292732)
    async def sell(self, ctx, type: str, symbol, amount):
        current_price = await get_stock_price(self, ctx, symbol)
        value = int(current_price) * int(amount)
        if int(value) > 10_000_000_000_000_000_000_000:
            embed = discord.Embed(description="Can only sell 10,000,000,000,000,000,000,000 of value at a time", color=discord.Color.red())
            await ctx.send(embed=embed)
            return
        if check_current_hp(ctx.author.id) == 0:
            embed = discord.Embed(description="Your HP is zero, Please Heal", color=discord.Color.red())
            await ctx.send(embed=embed)
            return
        if isinstance(amount, str):
            amount = int(amount.replace(",", ""))

        if len(self.transaction_pool) > 0:
            embed = discord.Embed(description=f"Transaction Pending, you have {len(self.transaction_pool)} ahead of you. ", color=discord.Color.yellow())
            await ctx.send(embed=embed)
        if await self.is_trading_halted() == True:
            await ctx.send("All Trading Temporarily Halted")
            return
        if await self.is_trading_halted_stock(symbol) == True:
            embed = discord.Embed(description=f"Trading Temporarily Halted for {symbol}", color=discord.Color.red())
            await ctx.send(embed=embed)
            return

        await check_store_addr(self, ctx)
        if type.lower() == "stock":
            await self.check_stock_change(ctx, symbol)
        user_id = ctx.author.id
        p3addr = get_p3_address(self.P3addrConn, user_id)

        try:
            print(f"Debug Sell Wrapper\nType: {type}\nAsset: {symbol}\nQuantity: {amount}\nUserID: {user_id}\nP3 Address: {p3addr}")
            async with self.transaction_lock:
                self.transaction_pool.append((ctx, type, symbol, amount, "sell"))

            if len(self.transaction_pool) != 0:
                print(f"Pool Size: {len(self.transaction_pool)}")
                print("Processing Transactions")
                await self.process_transactions(ctx)
                await autoliquidate(self, ctx)
                mv = await get_etf_value(self.conn, 6)
                await add_mv_metric(self, ctx, mv)
                await add_reserve_metric(self, ctx)
                mv_avg = await calculate_average_mv(self)
            print(f"MV Avg: {mv_avg:,.2f}")
        except ValueError:
            await ctx.send("Invalid amount. Please provide a valid integer.")

    @commands.command(name="casino", help="Buy Stocks, ETFs, and Items")
    async def casino(self, ctx, game: str, choice, amount):
        await check_store_addr(self, ctx)
        user_id = ctx.author.id
        p3addr = get_p3_address(self.P3addrConn, user_id)

        try:
            amount = int(amount.replace(",", ""))
            print(f"Debug Casino Wrapper\nGame: {game}\nChoice: {choice}\nBet: {amount}\nUserID: {user_id}\nP3 Address: {p3addr}")
            async with self.transaction_lock:
                self.transaction_pool.append((ctx, game, choice, amount, "casino"))
                print(f"Pool Size: {len(self.transaction_pool)}")
            if len(self.transaction_pool) == 1:
                print("Processing Transactions")
                await self.process_transactions()
        except ValueError:
            await ctx.send("Invalid amount. Please provide a valid integer.")


    @commands.command(name='reserve_chart')
    async def reserve_chart(self, ctx):
        try:
            cursor = self.conn.cursor()

            # Retrieve data for each metric
            cursor.execute("SELECT qse FROM reserve_value")
            qse_values = cursor.fetchall()

            cursor.execute("SELECT stocks FROM reserve_value")
            stocks_values = cursor.fetchall()

            cursor.execute("SELECT total FROM reserve_value")
            total_values = cursor.fetchall()

            if not qse_values and not stocks_values and not total_values:
                await ctx.send("No data available for the specified metrics.")
                return

            # Extract values from the result sets
            qse_values = [float(value[0]) for value in qse_values]
            stocks_values = [float(value[0]) for value in stocks_values]
            total_values = [float(value[0]) for value in total_values]

            # Calculate percentage change
            qse_percentage_change = np.array(qse_values) / qse_values[0] * 100 - 100
            stocks_percentage_change = np.array(stocks_values) / stocks_values[0] * 100 - 100
            total_percentage_change = np.array(total_values) / total_values[0] * 100 - 100

            # Create and save the chart with dark mode
            plt.style.use('dark_background')
            plt.figure(figsize=(20, 15))  # Adjust figure size as needed

            # Plot each metric
            plt.subplot(2, 1, 1)  # Two subplots, first one for raw values
            plt.plot(qse_values, label='QSE')
            plt.plot(stocks_values, label='Stocks')
            plt.plot(total_values, label='Total')

            plt.xlabel('Index', fontsize=14)
            plt.ylabel('Reserve Value', fontsize=14)
            plt.title('Reserve Value Chart (Raw Values)', fontsize=16)
            plt.legend(fontsize=12)
            plt.ticklabel_format(style='plain', useOffset=False, axis='y')
            plt.gca().yaxis.set_major_formatter(
                ticker.FuncFormatter(lambda x, _: "{:,.2f}".format(x))
            )
            plt.xticks(rotation=45, fontsize=12)
            plt.yticks(fontsize=12)

            # Plot percentage change
            plt.subplot(2, 1, 2)  # Two subplots, second one for percentage change
            plt.plot(qse_percentage_change, label='QSE')
            plt.plot(stocks_percentage_change, label='Stocks')
            plt.plot(total_percentage_change, label='Total')

            plt.xlabel('Index', fontsize=14)
            plt.ylabel('Percentage Change (%)', fontsize=14)
            plt.title('Reserve Value Chart (Percentage Change)', fontsize=16)
            plt.legend(fontsize=12)
            plt.ticklabel_format(style='plain', useOffset=False, axis='y')
            plt.xticks(rotation=45, fontsize=12)
            plt.yticks(fontsize=12)
            plt.ylim(min(qse_percentage_change.min(), stocks_percentage_change.min(), total_percentage_change.min()) - 100,
                     max(qse_percentage_change.max(), stocks_percentage_change.max(), total_percentage_change.max()) + 100)

            plt.tight_layout()  # Ensure proper layout spacing
            plt.savefig('reserve_chart.png')

            # Send the chart to the Discord channel
            with open('reserve_chart.png', 'rb') as file:
                chart_file = discord.File(file)
                await ctx.send(file=chart_file)

            # Create and send the embed
            embed = discord.Embed(title="Reserve Value COT", color=discord.Color.blue())
            embed.add_field(name="QSE", value=f"{qse_percentage_change[-1]:,.2f}%", inline=False)
            embed.add_field(name="Stocks", value=f"{stocks_percentage_change[-1]:,.2f}%", inline=False)
            embed.add_field(name="Total", value=f"{total_percentage_change[-1]:,.2f}%", inline=False)

            await ctx.send(embed=embed)

        except sqlite3.Error as e:
            print(f"SQLite error occurred: {str(e)}")
            await ctx.send("An error occurred while processing the chart.")
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")
            await ctx.send("An unexpected error occurred while processing the chart.")
        else:
            cursor.close()


    @commands.command(name='order_book_chart')
    async def order_book_chart(self, ctx, symbol: str):
        try:
            cursor = self.conn.cursor()

            # Retrieve buy orders
            cursor.execute("""
                SELECT price, SUM(quantity) as cumulative_quantity
                FROM limit_orders
                WHERE symbol = ? AND order_type = 'buy'
                GROUP BY price
                ORDER BY price DESC
            """, (symbol,))
            buy_data = cursor.fetchall()

            # Retrieve sell orders
            cursor.execute("""
                SELECT price, SUM(quantity) as cumulative_quantity
                FROM limit_orders
                WHERE symbol = ? AND order_type = 'sell'
                GROUP BY price
                ORDER BY price ASC
            """, (symbol,))
            sell_data = cursor.fetchall()

            if not buy_data and not sell_data:
                await ctx.send(f"No buy or sell orders available for {symbol}.")
                return

            # Calculate cumulative quantities for buy orders
            buy_prices, buy_cumulative_quantities = zip(*buy_data) if buy_data else ([], [])

            # Calculate cumulative quantities for sell orders
            sell_prices, sell_cumulative_quantities = zip(*sell_data) if sell_data else ([], [])

            # Plot the order book depth chart
            plt.style.use('dark_background')
            plt.figure(figsize=(20, 15))

            if buy_data:
                # Plot buy orders
                if sell_data:
                    # If both buy and sell data are available, use stacked bar chart
                    plt.bar(buy_prices, buy_cumulative_quantities, label='Buy Orders', color='green')
                else:
                    # If only buy data is available, use plot instead of bar
                    plt.plot(buy_prices, buy_cumulative_quantities, label='Buy Orders', color='green')

            if sell_data:
                # Plot sell orders on top of buy orders
                if buy_data:
                    # If both buy and sell data are available, use stacked bar chart
                    plt.bar(sell_prices, sell_cumulative_quantities, label='Sell Orders', color='red', bottom=buy_cumulative_quantities)
                else:
                    # If only sell data is available, use plot instead of bar
                    plt.plot(sell_prices, sell_cumulative_quantities, label='Sell Orders', color='red')

            plt.xlabel('Price')
            plt.ylabel('Cumulative Quantity')
            plt.title(f'Order Book Depth Chart - {symbol}')
            plt.legend()
            plt.grid(True)

            # Format y-axis labels with commas and two decimals
            plt.gca().yaxis.set_major_formatter(
                ticker.FuncFormatter(lambda x, _: "{:,.0f}".format(x))
            )

            # Format x-axis labels with non-scientific notation
            plt.ticklabel_format(style='plain', useOffset=False, axis='x')

            # Send the chart to the Discord channel
            plt.savefig('order_book_chart.png')
            plt.close()

            with open('order_book_chart.png', 'rb') as file:
                chart_file = discord.File(file)
                await ctx.send(file=chart_file)

        except sqlite3.Error as e:
            print(f"An error occurred: {str(e)}")
            await ctx.send("An error occurred while processing the chart.")




    @commands.command(name='market_chart')
    async def market_chart(self, ctx):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT mv FROM market_value")
            values = cursor.fetchall()

            if not values:
                await ctx.send("The market_value table is empty.")
                self.conn.close()
                return

            # Extract values from the result set
            values = [float(value[0]) for value in values]

            # Calculate RSI, Average Price, and Bollinger Bands
            rsi_period = 14  # Adjust as needed
            rsi_values = talib.RSI(np.array(values), timeperiod=rsi_period)
            average_price = np.mean(values)
            ath = np.max(values)
            atl = np.min(values)
            cmv = values[-1]  # Current Market Value

            # Calculate Moving Average (MA), Exponential Moving Average (EMA), and Simple Moving Average (SMA)
            ma_period = 20  # Adjust as needed
            ema_period = 20  # Adjust as needed
            ma_values = talib.MA(np.array(values), timeperiod=ma_period)
            ema_values = talib.EMA(np.array(values), timeperiod=ema_period)
            sma_values = talib.SMA(np.array(values), timeperiod=ma_period)

            # Calculate Rolling High and Low
            rolling_period = 14  # Adjust as needed
            rolling_high = np.maximum.accumulate(values)
            rolling_low = np.minimum.accumulate(values)

            # Calculate ROC and ATR
            roc_values = talib.ROC(np.array(values), timeperiod=1)  # Adjust as needed
            atr_values = talib.ATR(rolling_high, rolling_low, np.array(values), timeperiod=14)  # Adjust as needed

            # Calculate percentage change from ATL and ATH
            atl_percentage = ((np.array(values) - atl) / atl) * 100
            ath_percentage = ((np.array(values) - ath) / ath) * 100

            # Create and save the main chart with dark mode
            plt.style.use('dark_background')
            plt.figure(figsize=(20, 20))  # Adjust figure size for mobile

            # Plot market values
            plt.subplot(4, 1, 1)  # Reduce the number of subplots
            plt.plot(values, label=f'Market Value ({cmv:,.2f})', color='cyan')
            plt.plot(sma_values, label=f'SMA ({ma_period})', color='yellow', alpha=0.5)  # Add SMA line
            plt.axhline(y=average_price, color='cyan', linestyle='--', label=f'Avg Value ({average_price:,.2f})')
            plt.axhline(y=ath, color='green', linestyle=':', label=f'ATH ({ath:,.2f})')
            plt.axhline(y=atl, color='red', linestyle=':', label=f'ATL ({atl:,.2f})')
            plt.xlabel('Index', fontsize=14)
            plt.ylabel('Market Value', fontsize=14)
            plt.title('Market Value Chart', fontsize=16)
            plt.ticklabel_format(style='plain', useOffset=False, axis='y')  # Disable scientific notation
            # Format y-axis labels with commas and two decimals
            plt.gca().yaxis.set_major_formatter(
                ticker.FuncFormatter(lambda x, _: "{:,.2f}".format(x))
            )
            plt.xticks(rotation=45)
            plt.legend(fontsize=14, loc='best')

            # Plot RSI and ROC on the same subplot
            plt.subplot(4, 1, 2)  # Reduce the number of subplots
            plt.plot(rsi_values, label='RSI', color='orange', alpha=0.5)
            plt.axhline(y=70, color='r', linestyle='--', label='Overbought (70)')
            plt.axhline(y=30, color='g', linestyle='--', label='Oversold (30)')
            plt.plot(roc_values, label='ROC', color='cyan')  # Combine with RSI
            plt.xlabel('Index', fontsize=14)
            plt.ylabel('RSI / ROC', fontsize=14)
            plt.title('RSI and ROC', fontsize=14)
            plt.ticklabel_format(style='plain', useOffset=False, axis='y')
            plt.xticks(rotation=45)
            plt.legend(fontsize=14)

            # Plot Percentage Change from ATL and ATH
            plt.subplot(4, 1, 3)  # Add a new subplot
            plt.plot(atl_percentage, label='Percentage Change from ATL', color='red')
            plt.plot(ath_percentage, label='Percentage Change from ATH', color='green')
            plt.axhline(y=0, color='cyan', linestyle='--', label='Current Value')
            plt.ylabel('Percentage Change (%)', fontsize=14)
            plt.title('Percentage Change from ATL and ATH', fontsize=14)
            plt.ticklabel_format(style='plain', useOffset=False, axis='y')
            plt.xticks(rotation=45)
            plt.legend(fontsize=14, loc='best')

            # Plot ATR
            plt.subplot(4, 1, 4)  # Add a new subplot
            plt.plot(atr_values, label='ATR', color='yellow')
            plt.xlabel('Index', fontsize=14)
            plt.ylabel('ATR', fontsize=14)
            plt.title('ATR', fontsize=14)
            plt.ticklabel_format(style='plain', useOffset=False, axis='y')
            plt.xticks(rotation=45)
            plt.legend(fontsize=14, loc='best')


            plt.tight_layout()  # Ensure proper layout spacing
            plt.savefig('market_chart.png')
            plt.close()

            # Send the chart to the Discord channel
            with open('market_chart.png', 'rb') as file:
                chart_file = discord.File(file)
                await ctx.send(file=chart_file)

        except sqlite3.Error as e:
            print(f"An error occurred: {str(e)}")
            await ctx.send("An error occurred while processing the chart.")


    @commands.command(name="depth", help="Show buy/sell depth of a stock.")
    async def show_depth(self, ctx, symbol: str, users: bool = False, depth_limit: int = 10):
        try:
            cursor = self.conn.cursor()

            if users:

                # Fetch all buy orders for the specified symbol
                cursor.execute("""
                    SELECT price, SUM(quantity) as total_quantity FROM limit_orders
                    WHERE order_type = 'buy' AND symbol = ?
                    GROUP BY price
                """, (symbol,))
                buy_orders = cursor.fetchall()

                # Fetch all sell orders for the specified symbol
                cursor.execute("""
                    SELECT price, SUM(quantity) as total_quantity FROM limit_orders
                    WHERE order_type = 'sell' AND symbol = ?
                    GROUP BY price
                """, (symbol,))
                sell_orders = cursor.fetchall()
            else:
                # Fetch all buy orders for the specified symbol
                cursor.execute("""
                    SELECT price, SUM(quantity) as total_quantity FROM limit_orders
                    WHERE order_type = 'buy' AND symbol = ? AND user_id != ?
                    GROUP BY price
                """, (symbol, ctx.author.id))
                buy_orders = cursor.fetchall()

                # Fetch all sell orders for the specified symbol
                cursor.execute("""
                    SELECT price, SUM(quantity) as total_quantity FROM limit_orders
                    WHERE order_type = 'sell' AND symbol = ? AND user_id != ?
                    GROUP BY price
                """, (symbol, ctx.author.id))
                sell_orders = cursor.fetchall()
            # Calculate the overall market price using the full set of orders
            all_orders = buy_orders + sell_orders
            weighted_average_price = sum(order['price'] * order['total_quantity'] for order in all_orders) / sum(order['total_quantity'] for order in all_orders)

            def format_order(order):
                total_value = order['price'] * order['total_quantity']
                return f"Price: {order['price']:,.2f}\nTotal Quantity: {order['total_quantity']:,}\n"

            # Display only a limited number of orders based on the depth limit
            buy_depth_info = "\n".join([format_order(order) for order in buy_orders[:depth_limit]])
            sell_depth_info = "\n".join([format_order(order) for order in sell_orders[:depth_limit]])

            # Calculate the total value of the entire depth
            buy_value_depth = sum(order['price'] * order['total_quantity'] for order in buy_orders)
            sell_value_depth = sum(order['price'] * order['total_quantity'] for order in sell_orders)
            total_value_depth = buy_value_depth + sell_value_depth

            if users:
                embed = discord.Embed(title=f"Buy/Sell Depth for {symbol} with your orders", color=discord.Color.blue())
            else:
                embed = discord.Embed(title=f"Buy/Sell Depth for {symbol} w/out your orders", color=discord.Color.blue())
            embed.add_field(name="Buy Depth", value=buy_depth_info, inline=False)
            embed.add_field(name="Sell Depth", value=sell_depth_info, inline=False)
            embed.add_field(name="", value=f"-------------------------------------", inline=False)
            embed.add_field(name="Total Value of Buy Orders", value=f"{buy_value_depth:,.2f} QSE", inline=False)
            embed.add_field(name="Total Value of Sell Orders", value=f"{sell_value_depth:,.2f} QSE", inline=False)
            embed.add_field(name="Total Value of Depth", value=f"{total_value_depth:,.2f} QSE", inline=False)
            embed.add_field(name="Average Market Price", value=f"{weighted_average_price:,.2f} QSE", inline=False)
            result = await lowest_price_order(self, ctx, "sell", symbol)
            embed.add_field(name="Current Market Price", value=f"{result['price']:,.2f} QSE", inline=False)

            await ctx.send(embed=embed)
            await self.order_book_chart(ctx, symbol)

        except sqlite3.Error as e:
            embed = discord.Embed(description=f"An error occurred while fetching the buy/sell depth: {e}")
            await ctx.send(embed=embed)
        except ZeroDivisionError:
            embed = discord.Embed(description=f"No orders found for {symbol}")
            await ctx.send(embed=embed)
        except Exception as e:
            embed = discord.Embed(description=f"An error occurred while fetching the buy/sell depth: {e}")
            await ctx.send(embed=embed)


    @commands.command(name="send_from_reserve")
    @is_allowed_user(930513222820331590, PBot)
    async def send_from_reserve(self, ctx, user_id, amount: int):


        mv_avg = await calculate_average_mv(self)
        print(f"MV Avg: {mv_avg:,.2f}")
        reserve_funds = get_user_balance(self.conn, PBot)
        amount_percentage = (amount / reserve_funds) * 100
        if amount_percentage > 10:
            print(f"Emergency Minting {amount:,.2f} QSE to the Reserve")
            await mint_to_reserve(self, ctx, amount)
            print(f"Reserve {amount_percentage:,.2f}")
        else:
            print(f"Reserve {amount_percentage:,.2f}")
        # Check if the amount is over 100 trillion
        if amount > 9_000_000_000_000_000_000:
            chunks = (amount + 99_999_999_999_999_999_999) // 9_000_000_000_000_000_000  # Calculate the number of chunks
            chunk_amount = amount // chunks  # Calculate the amount per chunk
        else:
            chunks = 1
            chunk_amount = amount

        for _ in range(chunks):
            current_balance = get_user_balance(self.conn, user_id)
            print(f"{chunks:,.0f}:{chunk_amount:,.0f}")
            chunk_amount = Decimal(chunk_amount)
            new_balance = current_balance + chunk_amount
            await update_user_balance(self.conn, user_id, new_balance)

            reserve_balance = get_user_balance(self.conn, PBot)
            updated_balance = reserve_balance - chunk_amount
            await update_user_balance(self.conn, PBot, updated_balance)
            chunks -= 1



    @commands.command(name="cancel_order", help="Cancel an order by providing its order ID.")
#    @is_allowed_user(930513222820331590, PBot)
    async def cancel_order(self, ctx, order_id: int):
        try:
            cursor = self.conn.cursor()

            # Check if the order with the given ID exists and belongs to the user
            cursor.execute("""
                SELECT * FROM limit_orders
                WHERE order_id = ? AND user_id = ?
            """, (order_id, ctx.author.id))
            order = cursor.fetchone()

            if order:
                target = get_p3_address(self.P3addrConn, order["user_id"])
                if order["order_type"].lower() == "buy":
                    tax_percentage = self.calculate_tax_percentage(ctx, "buy_stock")
                    cost = order["quantity"] * order["price"]
                    fee = Decimal(cost) * Decimal(tax_percentage)
                    await self.send_from_reserve(ctx, ctx.author.id, (int(cost) - int(fee)))
                    print(f"""
                        Address: {target}
                        Refund: {cost:,}
                        Gas: {fee:,}
                    """)
                else:
                    sender_addr = get_p3_address(self.P3addrConn, PBot)
                    await send_stock(self, ctx, target, sender_addr, order["symbol"], order["quantity"], False)
#                    await self.give_stock(ctx, target, order["symbol"], order["quantity"], False)
                    print(f"""
                        Address: {target}
                        Refund Stock: {order["symbol"]}
                        Amount: {order["quantity"]:,}
                    """)

                # Perform the cancellation by deleting the order
                cursor.execute("""
                    DELETE FROM limit_orders
                    WHERE order_id = ?
                """, (order_id,))

                self.conn.commit()

                embed = discord.Embed(description=f"Order {order_id} successfully canceled.")
                await ctx.send(embed=embed)
            else:
                embed = discord.Embed(description="Either the order does not exist or you are not the owner of the order.")
                await ctx.send(embed=embed)

        except sqlite3.Error as e:
            embed = discord.Embed(description=f"An error occurred while canceling the order: {e}")
            await ctx.send(embed=embed)

    @commands.command(name="cancel_all_orders_for_symbol", help="Cancel all orders for a specified symbol.")
    #    @is_allowed_user(930513222820331590, PBot)
    async def cancel_all_orders_for_symbol(self, ctx, symbol: str):
        try:
            cursor = self.conn.cursor()

            # Get all orders for the current user and specified symbol
            cursor.execute("""
                SELECT * FROM limit_orders
                WHERE user_id = ? AND symbol = ?
            """, (ctx.author.id, symbol))
            orders = cursor.fetchall()

            if orders:
                for order in orders:
                    target = get_p3_address(self.P3addrConn, order["user_id"])
                    if order["order_type"].lower() == "buy":
                        tax_percentage = self.calculate_tax_percentage(ctx, "buy_stock")
                        cost = order["quantity"] * order["price"]
                        fee = Decimal(cost) * Decimal(tax_percentage)
                        await self.send_from_reserve(ctx, ctx.author.id, (int(cost) - int(fee)))
                        print(f"""
                            Address: {target}
                            Refund: {cost:,}
                            Gas: {fee:,}
                        """)
                    else:
                        sender_addr = get_p3_address(self.P3addrConn, PBot)
                        await send_stock(self, ctx, target, sender_addr, order["symbol"], order["quantity"], False)
                        print(f"""
                            Address: {target}
                            Refund Stock: {order["symbol"]}
                            Amount: {order["quantity"]:,}
                        """)

                    # Perform the cancellation by deleting the order
                    cursor.execute("""
                        DELETE FROM limit_orders
                        WHERE order_id = ?
                    """, (order["order_id"],))

                self.conn.commit()

                embed = discord.Embed(description=f"All orders for symbol {symbol} successfully canceled.")
                await ctx.send(embed=embed)
            else:
                embed = discord.Embed(description=f"No orders found for symbol {symbol} and the current user.")
                await ctx.send(embed=embed)

        except sqlite3.Error as e:
            embed = discord.Embed(description=f"An error occurred while canceling orders: {e}")
            await ctx.send(embed=embed)

    @commands.command(name="cancel_orders_for_symbol_price", help="Cancel orders for a specified symbol and price.")
    # @is_allowed_user(930513222820331590, PBot)
    async def cancel_orders_for_symbol_price(self, ctx, symbol: str, price: int = None):
        try:
            cursor = self.conn.cursor()

            # Get all orders for the current user, specified symbol, and optional price
            if price is not None:
                cursor.execute("""
                    SELECT * FROM limit_orders
                    WHERE user_id = ? AND symbol = ? AND price = ?
                """, (ctx.author.id, symbol, price))
            else:
                cursor.execute("""
                    SELECT * FROM limit_orders
                    WHERE user_id = ? AND symbol = ?
                """, (ctx.author.id, symbol))

            orders = cursor.fetchall()

            if orders:
                for order in orders:
                    target = get_p3_address(self.P3addrConn, order["user_id"])
                    if order["order_type"].lower() == "buy":
                        tax_percentage = self.calculate_tax_percentage(ctx, "buy_stock")
                        cost = order["quantity"] * order["price"]
                        fee = Decimal(cost) * Decimal(tax_percentage)
                        await self.send_from_reserve(ctx, ctx.author.id, (int(cost) - int(fee)))
                        print(f"""
                            Address: {target}
                            Refund: {cost:,}
                            Gas: {fee:,}
                        """)
                    else:
                        sender_addr = get_p3_address(self.P3addrConn, PBot)
                        await send_stock(self, ctx, target, sender_addr, order["symbol"], order["quantity"], False)
                        print(f"""
                            Address: {target}
                            Refund Stock: {order["symbol"]}
                            Amount: {order["quantity"]:,}
                        """)

                    # Perform the cancellation by deleting the order
                    cursor.execute("""
                        DELETE FROM limit_orders
                        WHERE order_id = ?
                    """, (order["order_id"],))

                self.conn.commit()

                embed = discord.Embed(description=f"All orders for symbol {symbol} and price {price} successfully canceled.")
                await ctx.send(embed=embed)
            else:
                embed = discord.Embed(description=f"No orders found for symbol {symbol}, price {price}, and the current user.")
                await ctx.send(embed=embed)

        except sqlite3.Error as e:
            embed = discord.Embed(description=f"An error occurred while canceling orders: {e}")
            await ctx.send(embed=embed)


    @commands.command(name="my_orders", help="Show your placed orders.")
    async def show_my_orders(self, ctx, symbol: str = None):
        try:
            cursor = self.conn.cursor()


            if symbol:
                # Fetch orders placed by the user
                cursor.execute("""
                    SELECT * FROM limit_orders
                    WHERE user_id = ? and symbol = ?
                    """, (ctx.author.id, symbol,))
                user_orders = cursor.fetchall()

            else:
                # Fetch orders placed by the user
                cursor.execute("""
                    SELECT * FROM limit_orders
                    WHERE user_id = ?
                    """, (ctx.author.id,))
                user_orders = cursor.fetchall()

            if not user_orders:
                embed = discord.Embed(description="You have no placed orders.")
                await ctx.send(embed=embed)
                return

            results_per_page = 5  # Adjust this based on how many results you want per page
            total_pages = (len(user_orders) + results_per_page - 1) // results_per_page

            for page_num in range(total_pages):
                start_index = page_num * results_per_page
                end_index = min((page_num + 1) * results_per_page, len(user_orders))

                embed = discord.Embed(title=f"Your Placed Orders - Page {page_num + 1}/{total_pages}", color=discord.Color.green())

                for index in range(start_index, end_index):
                    order = user_orders[index]

                    embed.add_field(name=f"Order ID: {order['order_id']}",
                                    value=f"Symbol: {order['symbol']}\nOrder Type: {order['order_type']}\nPrice: {order['price']:,.2f}\nQuantity: {order['quantity']:,.0f}",
                                    inline=False)

                message = await ctx.send(embed=embed)

                if total_pages > 1:
                    if page_num > 0:
                        await message.add_reaction("⬅️")  # Previous page

                    if page_num < total_pages - 1:
                        await message.add_reaction("➡️")  # Next page

                def check(reaction, user):
                    return user == ctx.author and reaction.message.id == message.id

                try:
                    reaction, user = await self.bot.wait_for('reaction_add', timeout=60.0, check=check)

                    if str(reaction.emoji) == "⬅️" and page_num > 0:
                        page_num -= 1
                    elif str(reaction.emoji) == "➡️" and page_num < total_pages - 1:
                        page_num += 1

                    await message.clear_reactions()
                except asyncio.TimeoutError:
                    break

        except sqlite3.Error as e:
            embed = discord.Embed(description=f"An error occurred while fetching your orders: {e}")
            await ctx.send(embed=embed)

    @commands.command(name="grab_orders")
    @is_allowed_user(930513222820331590, PBot)
    async def grab_orders(self, ctx, stock_name: str, type: str):
        if type.lower() == "sell":
            result = await lowest_price_order(self, ctx, "sell", stock_name)
        elif type.lower() == "buy":
            result = await lowest_price_order(self, ctx, "buy", stock_name)

        if result:
            return result
        else:
            return False


    @commands.command(name="simulate_fill_orders")
    async def transact_order2(self, ctx, symbol, quantity_to_buy: int):
        if quantity_to_buy > 500_000_000_000_000_000_000_000:
            await ctx.send(f"Purchase quantity can't be over 500.000.000,000,000,000,000,000")
            return
        buyer_id = ctx.author.id
        buyer_addr = get_p3_address(self.P3addrConn, buyer_id)
        cursor = self.conn.cursor()

        # Retrieve stock information
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (symbol,))
        stock = cursor.fetchone()

        if stock is None:
            await ctx.send(f"{ctx.author.mention}, this stock does not exist.")
            return

        # Check the existing amount of this stock owned by the user
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (symbol,))
        stock = cursor.fetchone()
        available, price, total_supply = int(stock[4]), Decimal(stock[2]), int(stock[3])
        cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (buyer_id, symbol))
        user_stock = cursor.fetchone()
        user_owned = int(user_stock[0]) if user_stock else 0
        user_escrow = await get_total_shares_user_order(self, buyer_id, symbol)

        if (user_owned + quantity_to_buy + user_escrow) > (total_supply * 0.51):
            embed = discord.Embed(description=f"{buyer_addr}, you cannot own more than 51% of the total supply of {symbol} stocks.\nAvailable: {available:,}\nTotal: {total_supply:,}\nYour Shares + Escrow: {user_owned:,} + {user_escrow:,}", color=discord.Color.red())
            await ctx.send(embed=embed)
            return

        current_timestamp = datetime.utcnow()


        is_staking_qse_genesis = self.is_staking_qse_genesis(buyer_id)


        market_price = await get_stock_price(self, ctx, symbol)
        color = discord.Color.green()
        embed = discord.Embed(title=f"Stock Transaction Processing", color=color)
        embed.add_field(name="Address:", value=f"{buyer_addr}", inline=False)
        embed.add_field(name="Stock:", value=f"{symbol}", inline=False)
        embed.add_field(name="Amount:", value=f"{quantity_to_buy:,.2f}", inline=False)
        lowest = await lowest_price_order(self, ctx, "sell", symbol)
        embed.add_field(name="Lowest Price:", value=f"{lowest['price']:,.2f}", inline=False)
        embed.add_field(name="Market Price:", value=f"{market_price:,.2f}", inline=False)
        current_balance = get_user_balance(self.conn, buyer_id)
        embed.add_field(name="Current Balance:", value=f"{current_balance:,.2f} $QSE", inline=False)

        embed.set_footer(text=f"Timestamp: {current_timestamp}")
        await ctx.send(embed=embed)

        # Fetch available sell orders
        sell_orders = await fetch_sell_orders(self, symbol)

        # Filter out orders owned by the buyer
        filtered_sell_orders = [order for order in sell_orders if order['user_id'] != buyer_id]

        # Early exit if no eligible sell orders
        if not filtered_sell_orders:
            embed = discord.Embed(
                title="No Available Sell Orders",
                description="There are no sell orders available from other users.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return

        # Iterate through sell orders
        orders_purchased = 0
        orders_removed = 0
        order_prices = []
        for order in filtered_sell_orders:  # Use the filtered list
            self.buy_timer_start = timeit.default_timer()
            order_id = order['order_id']
            seller_id = order['user_id']

            price = order['price']
            order_prices.append(price)
            available_quantity = order['quantity']



            # Simulate buying logic (adjust according to your requirements)
            quantity_to_fill = min(quantity_to_buy, available_quantity)
            total_cost = price * quantity_to_fill
            tax_percentage = self.calculate_tax_percentage(ctx, "buy_stock")
            fee = total_cost * tax_percentage
            total_cost_tax = total_cost + fee


            seller_addr = get_p3_address(self.P3addrConn, seller_id)
            escrow_addr = get_p3_address(self.P3addrConn, PBot)


            # Check if the buyer has enough balance
            buyer_balance = get_user_balance(self.conn, buyer_id)
            seller_old = get_user_balance(self.conn, seller_id)
            if buyer_balance >= total_cost_tax:
                await self.give_addr(ctx, seller_addr, int(total_cost - fee), False)
                add_city_tax(buyer_id, (fee / 2))
                add_city_tax(seller_id, (fee / 2))
                await send_stock(self, ctx, buyer_addr, escrow_addr, symbol, quantity_to_fill, False)
                seller_new = get_user_balance(self.conn, seller_id)
                new_balance = get_user_balance(self.conn, buyer_id)
                await log_transaction(ledger_conn, ctx, "Buy Stock", symbol, quantity_to_fill, total_cost, total_cost_tax, current_balance, new_balance, price, "False")
                await log_order_transaction(ledger_conn, ctx, "Sell Stock", symbol, quantity_to_fill, total_cost, (total_cost - fee), seller_old, seller_new, price, "False", seller_id)
                mv = await get_etf_value(self.conn, 6)
                await add_mv_metric(self, ctx, mv)
                await add_reserve_metric(self, ctx)
                mv_avg = await calculate_average_mv(self)
                print(f"MV Avg: {mv_avg:,.2f}")
 #       	result = await get_supply_stats(self, ctx, stock_name)
#        	reserve, total, locked, escrow, market, circulating = result
                avg_buy, avg_sell = await calculate_average_prices_by_symbol(self, symbol)
                avg_price = (avg_buy + avg_sell) / 2
                avg_price = ((avg_price * 0.05) * (1 - (available / total_supply)) * (1 - (quantity_to_fill / total_supply))) + avg_buy
                await self.change_stock_price(ctx, symbol, avg_price)


                await add_experience(self, self.conn, buyer_id, 1.5, ctx)
                await add_experience(self, self.conn, seller_id, 5, ctx)
                await autoliquidate(self, ctx)

                # Simulate updating order book
                if quantity_to_fill == available_quantity:
                    # Completely fill the order
                    remove_order(self.conn, order_id)
                    orders_removed += 1
                    print(f"Orders Removed: {orders_removed}\nQuantity to buy {quantity_to_fill:,.0f}")
                else:
                    # Partially fill the order
                    update_order_quantity(self.conn, order_id, available_quantity - quantity_to_fill)

                # Update remaining quantity to buy
                quantity_to_buy -= quantity_to_fill
                orders_purchased += 1

            # Check if all required quantity has been bought
            if quantity_to_buy <= 0:
                new_price = await get_stock_price(self, ctx, symbol)
                current_timestamp = datetime.utcnow()
                color = discord.Color.green()
                embed = discord.Embed(title=f"Stock Transaction Completed", color=color)
                embed.add_field(name="Address:", value=f"{buyer_addr}", inline=False)
                embed.add_field(name="Stock:", value=f"{symbol}", inline=False)
                total = sum(order_prices)
                average = total / len(order_prices)
                embed.add_field(name="Average Buy Price:", value=f"{average:,.2f}", inline=False)
                embed.add_field(name="Market Price:", value=f"{new_price:,.2f}({(((new_price - market_price) / market_price) * 100):,.5f}%)", inline=False)
                new_balance = get_user_balance(self.conn, buyer_id)
                embed.add_field(name="New Balance:", value=f"{new_balance:,.2f} $QSE", inline=False)
                embed.add_field(name="Orders Filled:", value=f"{orders_removed:,.0f}", inline=False)
                embed.add_field(name="Orders Purchased From:", value=f"{len(order_prices):,.0f}", inline=False)
                tax_rate = tax_percentage * 100
                embed.add_field(name="Gas Rate:", value=f"{tax_rate:.2f}%", inline=False)


                elapsed_time = timeit.default_timer() - self.buy_timer_start
                self.buy_stock_avg.append(elapsed_time)
                avg_time = sum(self.buy_stock_avg) / len(self.buy_stock_avg)
                embed.add_field(name="Transaction Time:", value=f"{elapsed_time:.2f} seconds", inline=False)
                embed.set_footer(text=f"Timestamp: {current_timestamp}")
                await ctx.send(embed=embed)
                break

    @commands.command(name="test_order")
    async def test_order(self, ctx, symbol, amount):
        sell_orders = await fetch_sell_orders(self, symbol)


        P3addrConn = sqlite3.connect("P3addr.db")

        for order in sell_orders:
            print(f"""
            {order['order_id']}
            {get_p3_address(P3addrConn, order['user_id'])}
            {order['price']}
            {order['quantity']}
            """)



    @commands.command(name="buy_stock", help="Buy stocks. Provide the stock name and amount.")
    @is_allowed_user(930513222820331590, PBot)
    async def buy_stock(self, ctx, stock_name: str, amount: int, stable_option: str = "False"):
        if amount == 0:
            await ctx.send(f"{ctx.author.mention}, you cannot buy 0 amount of {stock_name}.")
            return
        if amount <= 0:
            await ctx.send("Amount must be a positive number.")
            return

        price = await get_stock_price(self, ctx, stock_name)
        result = await get_supply_stats(self, ctx, stock_name)
        reserve, total, locked, escrow, market, circulating = result
        price = ((price * 0.05) * (1 - (market / total)) * (1 - (amount / total)) * (1 - (escrow / total))) + price
        cost = Decimal(price) * Decimal(amount)
        tax_percentage = self.calculate_tax_percentage(ctx, "buy_stock")
        fee = cost * Decimal(tax_percentage)
        total_cost = cost + fee

        current_timestamp = datetime.utcnow()
        self.last_buyers = [entry for entry in self.last_buyers if (current_timestamp - entry[1]).total_seconds() <= self.calculate_average_time_type("buy_stock")]
        user_id = ctx.author.id
        P3addrConn = sqlite3.connect("P3addr.db")
        P3addr = get_p3_address(P3addrConn, user_id)
        sender_addr = get_p3_address(self.P3addrConn, PBot)
        seller_old = get_user_balance(self.conn, PBot)
        is_staking_qse_genesis = self.is_staking_qse_genesis(user_id)
        color = discord.Color.green()
        embed = discord.Embed(title=f"Stock Transaction Processing", color=color)
        embed.add_field(name="Address:", value=f"{P3addr}", inline=False)
        embed.add_field(name="Stock:", value=f"{stock_name}", inline=False)
        embed.add_field(name="Quantity to Purchase:", value=f"{amount:,.0f}", inline=False)
        embed.add_field(name="Price:", value=f"{price:,.2f}", inline=False)
        embed.add_field(name="QSE to Pay:", value=f"{(Decimal(amount) * Decimal(price)):,.2f}", inline=False)
        embed.add_field(name="Gas to Pay:", value=f"{(fee):,.2f}", inline=False)
        embed.set_footer(text=f"Timestamp: {current_timestamp}")

        await ctx.send(embed=embed)

        cursor = self.conn.cursor()
        buyer_id = ctx.author.id
        member = ctx.guild.get_member(user_id)




        cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (PBot, stock_name))
        reserve_stock = cursor.fetchone()
        reserve_owned = int(reserve_stock[0]) if reserve_stock else 0

        if amount > reserve_owned:
            await ctx.send(f"Purchase amount: {amount:,.0f} is more than Market Supply: {reserve_owned:,.0f}")
        # Retrieve stock information
        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
        stock = cursor.fetchone()

        if stock is None:
            await ctx.send(f"{ctx.author.mention}, this stock does not exist.")
            return


        cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
        stock = cursor.fetchone()

        available, price, total_supply = int(stock[4]), Decimal(stock[2]), int(stock[3])
        escrow_supply = await get_total_shares_in_orders(self, stock_name)
        reserve_supply = self.get_user_stock_amount(PBot, stock_name)
        reserve_supply = reserve_supply - escrow_supply


        # Check the existing amount of this stock owned by the user
        cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (ctx.author.id, stock_name))
        user_stock = cursor.fetchone()
        user_owned = int(user_stock[0]) if user_stock else 0

        if (user_owned + amount) > (total_supply * 0.51):
            embed = discord.Embed(description=f"{ctx.author.mention}, you cannot own more than 51% of the total supply of {stock_name} stocks.\nAvailable: {reserve_supply:,}\nTotal: {total_supply:,}\nYour Shares: {user_owned:,}", color=discord.Color.red())
            await ctx.send(embed=embed)
            return

        if amount > reserve_supply:
            await ctx.send(f"{ctx.author.mention}, there are only {reserve_supply:,} {stock_name} stocks available.")
            return



        current_balance = get_user_balance(self.conn, ctx.author.id)

        if total_cost > current_balance:
            missing_amount = total_cost - current_balance
            await ctx.send(f"{ctx.author.mention}, you do not have enough $QSE to buy these stocks. "
                           f"You need {missing_amount:,.2f} more $QSE, including Gas, to complete this purchase.")
            return

        new_balance = current_balance - total_cost
        P3addrConn = sqlite3.connect("P3addr.db")
        PBotAddr = get_p3_address(P3addrConn, PBot)
        print(f"""Transaction Buy Market:

        {current_balance:,.2f}

        {new_balance:,.2f}

        {total_cost:,.2f}

        {(current_balance - total_cost):,.2f}
        """)
        await self.give_addr(ctx, PBotAddr, int(cost), False)
        user_id = ctx.author.id
        add_city_tax(user_id, fee)
        P3addrConn = sqlite3.connect("P3addr.db")
        await send_stock(self, ctx, P3addr, sender_addr, stock_name, amount, False)

        await record_user_daily_buy(cursor, ctx.author.id, stock_name, amount)
        decay_other_stocks(self.conn, stock_name)


        reserve_per = (locked / total) * 100
        print(f"""\n\n
            Stock: {stock_name}
            Locked: {locked:,}({reserve_per:,.10f}%)
            Market: {market:,}
            Reserve: {reserve:,}
            Escrow: {escrow:,}\n\n
        """)
        if reserve_per < Decimal(10) and market < amount:
            await self.unlock_shares(ctx, stock_name, amount, "lock")
            print(f"Locked {amount:,.0f} shares for {stock_name}")
        if reserve_per > Decimal(50):
            unlock_amount = (Decimal(0.01) / 100) * Decimal(total)
            await self.unlock_shares(ctx, stock_name, int(unlock_amount), "unlock")
            print(f"Unlocked {unlock_amount:,.0f} shares for {stock_name}")

        if stock_name == 'P3:Stable':
            await self.stableManager(ctx, 'BUY', amount)
        elif stock_name == 'ROFLStocks':
            await self.memeManager(ctx, 'BUY', amount)
        else:
            a = 1
#            await self.increase_price(ctx, stock_name, amount)

        if amount == 0:
            print(f'Fix for 0 amount buy after order book')
        else:
            seller_new = get_user_balance(self.conn, PBot)
            await log_transaction(ledger_conn, ctx, "Buy Stock", stock_name, amount, cost, total_cost, current_balance, new_balance, price, "False")
            await log_order_transaction(ledger_conn, ctx, "Sell Stock", stock_name, amount, total_cost, (cost), seller_old, seller_new, price, "False", PBot)
            await log_transfer(self, ledger_conn, ctx, "P3 Bot", self.bot_address, get_user_id(self.P3addrConn, self.bot_address), fee)
        avg_buy, avg_sell = await calculate_average_prices_by_symbol(self, stock_name)
        avg_price = (avg_buy + avg_sell) / 2
        await self.change_stock_price(ctx, stock_name, avg_price)
        current_timestamp = datetime.utcnow()
        color = discord.Color.green()
        embed = discord.Embed(title=f"Stock Transaction Completed", color=color)
        embed.add_field(name="Address:", value=f"{P3addr}", inline=False)
        embed.add_field(name="Stock:", value=f"{stock_name}", inline=False)
        embed.add_field(name="Quantity Purchased:", value=f"{amount:,.0f}", inline=False)
        new_price = await get_stock_price(self, ctx, stock_name)
        new_price = (new_price * 0.05) + new_price
        embed.add_field(name="New Price:", value=f"{new_price:,.2f}({(((int(new_price) - int(price)) / int(price)) * 100):,.5f}%)", inline=False)
        embed.add_field(name="Old Balance:", value=f"{current_balance:,.2f} $QSE", inline=False)
        embed.add_field(name="New Balance:", value=f"{new_balance:,.2f} $QSE", inline=False)
        tax_rate = tax_percentage * 100
        embed.add_field(name="Gas Rate:", value=f"{tax_rate:.2f}%", inline=False)
        elapsed_time = timeit.default_timer() - self.buy_timer_start
        embed.add_field(name="Transaction Time:", value=f"{elapsed_time:.2f} seconds", inline=False)
        embed.set_footer(text=f"Timestamp: {current_timestamp}")

        await ctx.send(embed=embed)
        await add_experience(self, self.conn, ctx.author.id, 1, ctx)
        self.conn.commit()

        try:
            elapsed_time = timeit.default_timer() - self.buy_timer_start
            self.buy_stock_avg.append(elapsed_time)
            avg_time = sum(self.buy_stock_avg) / len(self.buy_stock_avg)
            print(f"""
                -------------------------------------
                Transaction Time: {elapsed_time:.2f}
                Average Transaction Time: {avg_time:.2f}
            """)
        except Exception as e:
            print(f"Timer error: {e}")


# Sell Stock
    @commands.command(name="sell_stock", help="Sell stocks. Provide the stock name and amount.")
    @is_allowed_user(930513222820331590, PBot)
    async def sell_stock(self, ctx, stock_name: str, amount: int):
        result = await get_supply_stats(self, ctx, stock_name)
        reserve, total, locked, escrow, market, circulating = result
        price = await get_stock_price(self, ctx, stock_name)
        current_timestamp = datetime.utcnow()
        self.last_sellers = [entry for entry in self.last_sellers if (current_timestamp - entry[1]).total_seconds() <= self.calculate_average_time_type("sell_stock")]
        user_id = ctx.author.id
        sender_addr = get_p3_address(self.P3addrConn, ctx.author.id)
        seller_old = get_user_balance(self.conn, PBot)
        P3addrConn = sqlite3.connect("P3addr.db")
        P3addr = get_p3_address(P3addrConn, user_id)
        price = price - ((price * 0.03) * (1 - (market / total)) * (1 - (amount / total) * (1 - (escrow / total))))
        earnings = Decimal(price) * Decimal(amount)
        tax_percentage = self.calculate_tax_percentage(ctx, "sell_stock")  # Custom function to determine the tax percentage based on quantity and earnings
        fee = earnings * Decimal(tax_percentage)
        total_earnings = earnings - fee
        color = discord.Color.red()
        embed = discord.Embed(title=f"Stock Transaction Processing", color=color)
        embed.add_field(name="Address:", value=f"{P3addr}", inline=False)
        embed.add_field(name="Stock:", value=f"{stock_name}", inline=False)
        embed.add_field(name="Quantity to Sell:", value=f"{amount:,.0f}", inline=False)
        embed.add_field(name="Price:", value=f"{price:,.2f}", inline=False)
        embed.add_field(name="QSE to recieve:", value=f"{(Decimal(amount) * Decimal(price)):,.2f}", inline=False)
        embed.add_field(name="Gas to Pay:", value=f"{fee:,.2f}", inline=False)
        embed.set_footer(text=f"Timestamp: {current_timestamp}")


        await ctx.send(embed=embed)

        stock_price = await get_stock_price(self, ctx, stock_name)

        member = ctx.guild.get_member(user_id)
        # Check if the user is already in a transaction
        # Call the check_impact function to handle impact calculation and confirmation
        cursor = self.conn.cursor()


        if self.check_last_action(self.last_sellers, user_id, current_timestamp):

            return
            # Add the current action to the list
        self.last_sellers.append((user_id, current_timestamp))
        result = "bugged"

        # Check the result and perform further actions if needed
        if result == "canceled":
            # Handle canceled transaction
            pass
        elif result == "ongoing_transaction":
            # Handle ongoing transaction
            pass
        else:


            if amount <= 0:
                await ctx.send("Amount must be a positive number.")
                return



            await ctx.message.delete()



            # Check if the user recently bought stocks
            cursor.execute("SELECT timestamp FROM user_daily_buys WHERE user_id=? AND symbol=? ORDER BY timestamp DESC LIMIT 1", (user_id, stock_name))
            last_buy_timestamp = cursor.fetchone()

            if last_buy_timestamp:
                last_buy_timestamp = datetime.strptime(last_buy_timestamp[0], "%Y-%m-%d %H:%M:%S")
                current_time = datetime.utcnow()
                time_difference = current_time - last_buy_timestamp

                if time_difference.total_seconds() < 1800:  # 1800 seconds = 30 minutes
                    remaining_time = timedelta(seconds=1800) - time_difference
                    if user_id != jacob:
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

            current_balance = get_user_balance(self.conn, ctx.author.id)
            new_balance = current_balance + total_earnings
            user_id = ctx.author.id
            add_city_tax(user_id, fee)

            try:
                await self.send_from_reserve(ctx, user_id, int(total_earnings))
            except ValueError as e:
                await ctx.send(f"{ctx.author.mention}, an error occurred while updating the user balance. Error: {str(e)}")
                return



            # Proceed with the sell order if there's still remaining amount to sell
            if amount > 0:

                PBotAddr = get_p3_address(self.P3addrConn, PBot)
                await send_stock(self, ctx, PBotAddr, sender_addr, stock_name, int(amount), False)

                decay_other_stocks(self.conn, stock_name)
                reserve_per = (locked / total) * 100
                print(f"""\n\n
                    Stock: {stock_name}
                    Locked: {locked:,}({reserve_per:,.10f}%)
                    Market: {market:,}
                    Reserve: {reserve:,}
                    Escrow: {escrow:,}\n\n
                """)

                if reserve_per < Decimal(10) and locked < amount:
                    await self.unlock_shares(ctx, stock_name, amount, "lock")
                    print(f"Locked {amount:,.0f} shares for {stock_name}")
                if reserve_per > Decimal(50) and market < amount:
                    unlock_amount = (Decimal(0.01) / 100) * Decimal(total)
                    await self.unlock_shares(ctx, stock_name, int(unlock_amount), "unlock")
                    print(f"Unlocked {unlock_amount:,.0f} shares for {stock_name}")

                if stock_name == ('P3:Stable'):
                    await self.stableManager(ctx, 'sell', amount)

                elif stock_name == ('ROFLStocks'):
                    await self.memeManager(ctx, 'sell', amount)
                else:
                    a = 1
                seller_new = get_user_balance(self.conn, PBot)
                await log_transaction(ledger_conn, ctx, "Sell Stock", stock_name, amount, earnings, total_earnings, current_balance, new_balance, price, "False")
                await log_order_transaction(ledger_conn, ctx, "Buy Stock", stock_name, amount, total_earnings, (earnings), seller_old, seller_new, price, "False", PBot)
                await log_transfer(self, ledger_conn, ctx, "P3 Bot", self.bot_address, get_user_id(self.P3addrConn, self.bot_address), fee)
                self.conn.commit()
                user_id = ctx.author.id
                P3addrConn = sqlite3.connect("P3addr.db")
                P3addr = get_p3_address(P3addrConn, user_id)
                current_timestamp = datetime.utcnow()
                avg_buy, avg_sell = await calculate_average_prices_by_symbol(self, stock_name)
                avg_price = (avg_buy + avg_sell) / 2
                await self.change_stock_price(ctx, stock_name, avg_price)
                color = discord.Color.red()
                embed = discord.Embed(title=f"Stock Transaction Completed", color=color)
                embed.add_field(name="Address:", value=f"{P3addr}", inline=False)
                embed.add_field(name="Stock:", value=f"{stock_name}", inline=False)
                embed.add_field(name="Quantity Sold:", value=f"{amount:,.0f}", inline=False)
                new_price = await get_stock_price(self, ctx, stock_name)
                new_price = new_price - (new_price * 0.03)
                embed.add_field(name="New Price:", value=f"{new_price:,.2f}({(((int(new_price) - int(price)) / int(price)) * 100):,.5f}%)", inline=False)
                embed.add_field(name="Old Balance:", value=f"{current_balance:,.2f} $QSE", inline=False)
                embed.add_field(name="New Balance:", value=f"{new_balance:,.2f} $QSE", inline=False)
                tax_rate = tax_percentage * 100
                embed.add_field(name="Gas Rate:", value=f"{tax_rate:.2f}%", inline=False)
                elapsed_time = timeit.default_timer() - self.sell_timer_start
                embed.add_field(name="Transaction Time:", value=f"{elapsed_time:.2f} seconds", inline=False)
                embed.set_footer(text=f"Timestamp: {current_timestamp}")

                await ctx.send(embed=embed)
                await add_experience(self, self.conn, ctx.author.id, 5, ctx)

                self.sell_stock_avg.append(elapsed_time)
                avg_time = sum(self.sell_stock_avg) / len(self.sell_stock_avg)
                print(f"""
                -------------------------------------
                Transaction Time: {elapsed_time:.2f}
                Average Transaction Time: {avg_time:.2f}
                """)



    @commands.command(name='vacuum')
    @is_allowed_user(930513222820331590)
    async def vacuum_database(self, ctx):
        try:
            self.halt_trading = True
            embed = discord.Embed(description=f"Trading Halted for Database Vaccum", color=discord.Color.yellow())
            await ctx.send(embed=embed)
            embed = discord.Embed(description=f"Currency System Vaccum Started", color=discord.Color.yellow())
            await ctx.send(embed=embed)
            conn = sqlite3.connect("currency_system.db")
            conn.execute("VACUUM;")
            conn.close()
            embed = discord.Embed(description=f"Currency System Vaccum Succesful", color=discord.Color.green())
            await ctx.send(embed=embed)
            embed = discord.Embed(description=f"Address Database Vaccum Started", color=discord.Color.yellow())
            await ctx.send(embed=embed)
            conn = sqlite3.connect("P3addr.db")
            conn.execute("VACUUM;")
            conn.close()
            embed = discord.Embed(description=f"Address Database Vaccum Succesful", color=discord.Color.green())
            await ctx.send(embed=embed)
            embed = discord.Embed(description=f"Ledger Database Vaccum Started", color=discord.Color.yellow())
            await ctx.send(embed=embed)
            conn = sqlite3.connect("p3ledger.db")
            conn.execute("VACUUM;")
            conn.close()
            embed = discord.Embed(description=f"Ledger Database Vaccum Succesful", color=discord.Color.green())
            await ctx.send(embed=embed)
        except Exception as e:
            await ctx.send(f"An error occurred while vacuuming the database: {str(e)}")
        finally:
            self.halt_trading = False
            embed = discord.Embed(description=f"Trading Resumed, Database Vacuum Completed", color=discord.Color.green())
            await ctx.send(embed=embed)

    @commands.command(name="my_stocks", help="Shows the user's stocks.")
    async def my_stocks(self, ctx, stock_name: str = None):
        async with self.db_semaphore:
            async with self.transaction_lock:
                user_id = ctx.author.id
                P3Addr = generate_crypto_address(ctx.author.id)
                await ctx.message.delete()
                await ctx.send(f"Loading Stock Portflio for {P3Addr}...")

                cursor = self.conn.cursor()

                cursor.execute("SELECT symbol, amount FROM user_stocks WHERE user_id=? AND amount > 0", (user_id,))
                user_stocks = cursor.fetchall()

                if not user_stocks:
                    await ctx.send(f"{ctx.author.mention}, you do not own any stocks.")
                    return

                latest_price = await get_stock_price(self, ctx, stock_name)



                if stock_name:
                    # Display daily, weekly, and monthly average buy and sell prices for a specific stock
                    opening_price, current_price, interval_change = await get_stock_price_interval(self, ctx, stock_name, interval='daily')
                    weekly_opening_price, weekly_current_price, weekly_interval_change = await get_stock_price_interval(self, ctx, stock_name, interval='weekly')
                    monthly_opening_price, monthly_current_price, monthly_interval_change = await get_stock_price_interval(self, ctx, stock_name, interval='monthly')

                    # Convert prices to strings and remove commas
                    opening_price_str = f"${opening_price:,.2f}" if opening_price is not None else "0.00 QSE"
                    weekly_opening_price_str = f"${weekly_opening_price:,.2f}" if weekly_opening_price is not None else "0.00 QSE"
                    monthly_opening_price_str = f"${monthly_opening_price:,.2f}" if monthly_opening_price is not None else "0.00 QSE"

                    embed = discord.Embed(
                        title=f"Stock Performance - {stock_name}",
                        description=f"Performance summary for {P3Addr}",
                        color=discord.Color.green()
                    )

                    embed.add_field(name="Current Price: ", value=f"${latest_price:,.2f}", inline=False)
                    # Include the amount owned for the specific stock
                    cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (user_id, stock_name))
                    amount_owned = cursor.fetchone()
                    if amount_owned:
                        result = await get_supply_stats(self, ctx, stock_name)
                        reserve, total, locked, escrow, market, circulating = result
                        hold_percentage = (amount_owned[0] / total) * 100
                        if hold_percentage < 0.1:
                            hold_decimal = 8
                        else:
                            hold_decimal = 2
                        embed.add_field(name="Amount Owned", value=f"{amount_owned[0]:,} ({hold_percentage:,.{hold_decimal}f}%)", inline=False)
                        embed.add_field(name="Value", value=f"{(amount_owned[0] * latest_price):,.2f}", inline=False)
                    embed.add_field(name="Daily Average Buy Price", value=opening_price_str, inline=False)
                    embed.add_field(name="Daily Average Sell Price", value=f"{current_price:,.2f} QSE", inline=False)
                    embed.add_field(name="Weekly Average Buy Price", value=weekly_opening_price_str, inline=False)
                    embed.add_field(name="Weekly Average Sell Price", value=f"{weekly_current_price:,.2f} QSE", inline=False)
                    embed.add_field(name="Monthly Average Buy Price", value=monthly_opening_price_str, inline=False)
                    embed.add_field(name="Monthly Average Sell Price", value=f"{monthly_current_price:,.2f} QSE", inline=False)

                    await ctx.send(embed=embed)
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
                        stock_symbol = stock['symbol']
                        amount_owned = stock['amount']
                        opening_price, current_price, interval_change = await get_stock_price_interval(self, ctx, stock_symbol, interval='daily')
                        latest_price = await get_stock_price(self, ctx, stock_symbol)
                        share_value = amount_owned * latest_price


                        result = await get_supply_stats(self, ctx, stock_symbol)
                        reserve, total, locked, escrow, market, circulating = result
                        hold_percentage = (amount_owned / total) * 100
                        if hold_percentage < 0.1:
                            hold_decimal = 8
                        else:
                            hold_decimal = 2

                        # Convert opening_price to a string and remove commas
                        opening_price_str = f"{opening_price:,.2f} QSE" if opening_price is not None else "0.00 QSE"

                        embed.add_field(
                            name=f"{stock_symbol} (Amount: {amount_owned:,} )",
                            value=f"* Current Price: {latest_price:,.2f} QSE\n* Holding Percentage: ({hold_percentage:,.{hold_decimal}f}%)\n* Daily Average Buy Price: {opening_price_str}\n* Daily Average Sell Price: {current_price:,.2f} QSE\n* Total Value: ${share_value:,.2f}",
                            inline=True
                        )

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
                    reaction, user = await self.bot.wait_for("reaction_add", timeout=120.0, check=check)
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


    async def get_stock_info(self, ctx, stock):
        result = await lowest_price_order(self, ctx, "sell", stock[0])
        lowest_sell_price = result["price"] if result else None
        escrow_supply = await get_total_shares_in_orders(self, stock[0])
        reserve_supply = self.get_user_stock_amount(PBot, stock[0])
        reserve_supply = reserve_supply - escrow_supply

        lowest_sell_price_formatted = f"{lowest_sell_price:,.2f}" if lowest_sell_price is not None else "N/A"

        stock_info = (
            f"{stock[0]}\n"
            f"Available: {reserve_supply:,.0f}\n"
            f"Circulating: {int(stock[2] - reserve_supply):,.0f}\n"
            f"Total Shares in Escrow: {escrow_supply:,.0f}\n"
            f"Market Price: {stock[3]:,.2f}\n"
            f"Order Price: {lowest_sell_price_formatted} QSE"
        )
        return stock_info

    @commands.command(name='list_stocks', aliases=["stocks"])
    async def list_stocks(self, ctx):
        async with self.db_semaphore:
            async with self.transaction_lock:
                await ctx.message.delete()
                cursor = self.conn.cursor()
                cursor.execute("SELECT symbol, available, total_supply, price FROM stocks")
                stocks = cursor.fetchall()

        items_per_page = 10  # Number of stocks to display per page
        total_pages = (len(stocks) + items_per_page - 1) // items_per_page

        embed = discord.Embed(title="Quantum Stock Exchange", color=discord.Color.blue())
        embed.set_footer(text="Page 1 of {}".format(total_pages))

        page = 1
        start_index = (page - 1) * items_per_page
        end_index = start_index + items_per_page

        for stock in stocks[start_index:end_index]:
            stock_info = await self.get_stock_info(ctx, stock)
            embed.add_field(name="Stock:", value=stock_info, inline=False)

        message = await ctx.send(embed=embed)

        if total_pages > 1:
            await message.add_reaction("⬅️")
            await message.add_reaction("➡️")

            def check(reaction, user):
                return user == ctx.author and str(reaction.emoji) in ["⬅️", "➡️"]

            while True:
                try:
                    reaction, user = await self.bot.wait_for("reaction_add", timeout=120.0, check=check)
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
                    stock_info = await self.get_stock_info(ctx, stock)
                    embed.add_field(name="Stock:", value=stock_info, inline=False)

                embed.set_footer(text="Page {} of {}".format(page, total_pages))
                await message.edit(embed=embed)
                await message.remove_reaction(reaction, user)

        # Remove reactions at the end
        await message.clear_reactions()

# Stock Tools

    @commands.command(name='topstocks', help='Shows the top stocks based on specified criteria.')
    async def topstocks(self, ctx, option: str = 'mixed'):
        cursor = self.conn.cursor()

        try:
            if option.lower() == 'high':
                # Get the top 10 highest price stocks with available quantities
                cursor.execute("SELECT symbol, price, available FROM stocks WHERE available > 0 ORDER BY price DESC LIMIT 10")
                top_stocks = cursor.fetchall()
                title = 'Top 10 Highest Price Stocks'

            elif option.lower() == 'low':
                # Get the top 10 lowest price stocks with available quantities
                cursor.execute("SELECT symbol, price, available FROM stocks WHERE available > 0 ORDER BY price ASC LIMIT 10")
                top_stocks = cursor.fetchall()
                title = 'Top 10 Lowest Price Stocks'

            else:
                # Get the top 5 highest price stocks and the top 5 lowest price stocks with available quantities
                cursor.execute("SELECT symbol, price, available FROM stocks WHERE available > 0 ORDER BY price DESC LIMIT 5")
                top_high_stocks = cursor.fetchall()
                cursor.execute("SELECT symbol, price, available FROM stocks WHERE available > 0 ORDER BY price ASC LIMIT 5")
                top_low_stocks = cursor.fetchall()
                top_stocks = top_high_stocks + top_low_stocks
                title = 'Top 5 Highest and Lowest Price Stocks'

            # Create the embed
            embed = discord.Embed(title=title, color=discord.Color.blue())

            # Add fields for the top stocks
            for i, (symbol, price, available) in enumerate(top_stocks, start=1):
                decimal_places = 8 if price < 0.01 else 2
                embed.add_field(
                    name=f"#{i}: {symbol}",
                    value=f"Price: {price:,.{decimal_places}f} $QSE\nAvailable: {available:,}",
                    inline=False
                )

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

        current_price = await get_stock_price(self, ctx, stock_symbol)
        embed = discord.Embed(
            title="Treasure Chest",
            description=(f"""React with 💰 to claim your potential reward of {treasureMin:,} to {treasureMax:,}
            Stock: {stock_symbol}
            Current Price: {current_price:,.2f}
            """),
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

                await log_stock_transfer(self, ledger_conn, ctx, self.bot.user, user, stock_symbol, reward_amount)

                self.conn.commit()

                # Add the user to the set of claimed users
                self.claimed_users.add(user.id)

                #Boost P3:Treasure_Chest
                await self.treasureBooster(ctx)

        except asyncio.TimeoutError:
            await ctx.send("The treasure chest has been closed. Try again later!")



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
        create_user_rpg_stats(user_id)

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

    @commands.command(name="top_sellers")
    async def top_sellers(self, ctx):
        # Connect to the ledger database
        with sqlite3.connect("p3ledger.db") as ledger_conn:
            ledger_cursor = ledger_conn.cursor()

            current_date = datetime.now()

            # Calculate the first day of the current month
            first_day_of_current_month = current_date.replace(day=1)

            # Calculate one month ago from the first day of the current month
            one_month_ago = first_day_of_current_month - timedelta(days=1)

            # Retrieve sell transactions for all stocks from the ledger within the last month
            sell_transactions = ledger_cursor.execute("""
                SELECT symbol, timestamp, price, user_id
                FROM stock_transactions
                WHERE action='Sell Stock' AND timestamp >= ?
                ORDER BY timestamp
            """, (one_month_ago,))

            # Create a dictionary to store sellers and their total sell amounts
            seller_totals = {}

            for symbol, timestamp, price, seller_id in sell_transactions:
                # Update or initialize the total sell amount for the seller
                if not isinstance(price, float):
                    price = Decimal(price.replace(",", ""))
                seller_totals[seller_id] = seller_totals.get(seller_id, Decimal(0)) + Decimal(price)

            # Get the top 10 sellers by total sell amount across all stocks
            top_sellers = sorted(seller_totals.items(), key=lambda x: x[1], reverse=True)[:10]

            # Create an embed
            embed = Embed(title="Top Sellers", color=0x00ff00)

            # Add information to the embed
            for seller_id, total_sell_amount in top_sellers:
                p3addr = get_p3_address(self.P3addrConn, seller_id)
                embed.add_field(name=f"Seller {p3addr}", value=f"Total Sell Amount - {total_sell_amount:,.0f}", inline=False)

            # Send the embed to the channel
            await ctx.send(embed=embed)


    @commands.command(name="top_buyers")
    async def top_buyers(self, ctx):
        # Connect to the ledger database
        with sqlite3.connect("p3ledger.db") as ledger_conn:
            ledger_cursor = ledger_conn.cursor()

            current_date = datetime.now()

            # Calculate the first day of the current month
            first_day_of_current_month = current_date.replace(day=1)

            # Calculate one month ago from the first day of the current month
            one_month_ago = first_day_of_current_month - timedelta(days=1)

            # Retrieve sell transactions for all stocks from the ledger within the last month
            sell_transactions = ledger_cursor.execute("""
                SELECT symbol, timestamp, price, user_id
                FROM stock_transactions
                WHERE action='Buy Stock' AND timestamp >= ?
                ORDER BY timestamp
            """, (one_month_ago,))

            # Create a dictionary to store sellers and their total sell amounts
            seller_totals = {}

            for symbol, timestamp, price, seller_id in sell_transactions:
                # Update or initialize the total sell amount for the seller
                if not isinstance(price, float):
                    price = Decimal(price.replace(",", ""))
                seller_totals[seller_id] = seller_totals.get(seller_id, Decimal(0)) + Decimal(price)

            # Get the top 10 sellers by total sell amount across all stocks
            top_sellers = sorted(seller_totals.items(), key=lambda x: x[1], reverse=True)[:10]

            # Create an embed
            embed = Embed(title="Top Buyers", color=0x00ff00)

            # Add information to the embed
            for seller_id, total_sell_amount in top_sellers:
                p3addr = get_p3_address(self.P3addrConn, seller_id)
                embed.add_field(name=f"Buyer {p3addr}", value=f"Total Buy Amount - {total_sell_amount:,.0f}", inline=False)

            # Send the embed to the channel
            await ctx.send(embed=embed)

##

    @commands.command(name="get_total_transactions", help="Get the total of all transactions for the current month for a specified receiver_id.")
    async def get_total_transactions(ctx, receiver_id: int):
        # Connect to the p3ledger.db
        conn = sqlite3.connect("p3ledger.db")
        cursor = conn.cursor()

        try:
            # Calculate the start and end dates for the current month
            now = datetime.now()
            start_of_month = datetime(now.year, now.month, 1)
            end_of_month = start_of_month + timedelta(days=31)

            # Retrieve the total of all transactions for the current month and specified receiver_id
            cursor.execute("""
                SELECT COALESCE(SUM(amount), 0) AS total_amount
                FROM transfer_transactions
                WHERE receiver_id = ? AND timestamp >= ? AND timestamp < ?
            """, (receiver_id, start_of_month, end_of_month))

            total_amount = cursor.fetchone()[0]

            await ctx.send(f"The total transactions for {ctx.message.author.mention} this month is: {total_amount:,.2f} $QSE.")

        except Exception as e:
            await ctx.send(f"An error occurred: {e}")

        finally:
            # Close the connection
            conn.close()

    @commands.command(name="give_stock", help="Give a user an amount of a stock. Deducts it from PBot's holdings.")
    @is_allowed_user(930513222820331590)  # The user must have the 'admin' role to use this command.
    async def give_stock(self, ctx, target, symbol: str, amount: int, verbose: bool = True):
        try:
            cursor = self.conn.cursor()

            if target.startswith("P3:"):
                p3_address = target
                user_id = get_user_id(self.P3addrConn, p3_address)
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

            # Deduct the stock from PBot's holdings.
            pbot_stock = cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (PBot, symbol)).fetchone()
            if pbot_stock and pbot_stock[0] >= amount:
                new_pbot_amount = pbot_stock[0] - amount
                cursor.execute("UPDATE user_stocks SET amount=? WHERE user_id=? AND symbol=?", (new_pbot_amount, PBot, symbol))

                # Update the user's stocks.
                cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (user_id, symbol))
                user_stock = cursor.fetchone()
                if user_stock is None:
                    cursor.execute("INSERT INTO user_stocks(user_id, symbol, amount) VALUES(?, ?, ?)", (user_id, symbol, amount))
                else:
                    new_amount = user_stock[0] + amount
                    cursor.execute("UPDATE user_stocks SET amount=? WHERE user_id=? AND symbol=?", (new_amount, user_id, symbol))

                self.conn.commit()

                if verbose:
                    await ctx.send(f"Gave {amount:,} of {symbol} to {target}.")
            else:
                await ctx.send(f"Not enough {symbol} available in Market Holdings.")
        except Exception as e:
            print(f"An error occurred in give_stock: {e}")
            await ctx.send("An error occurred while processing the command.")






    @commands.command(name="distribute", help="Distribute a specified amount of a stock to all holders.")
    @is_allowed_user(930513222820331590)
    async def distribute_stock(self, ctx, symbol: str, amount: int):
        cursor = self.conn.cursor()

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

        # Get all users holding the specified stock.
        cursor.execute("SELECT user_id, amount FROM user_stocks WHERE symbol=?", (symbol,))
        users_stocks = cursor.fetchall()

        # Distribute the specified amount to each user.
        for user_stock in users_stocks:
            user_id, user_amount = user_stock['user_id'], user_stock['amount']
            new_user_amount = user_amount + amount
            cursor.execute("UPDATE user_stocks SET amount=? WHERE user_id=? AND symbol=?", (new_user_amount, user_id, symbol))

        # Deduct the stock from the total supply.
        new_available = stock['available'] - (amount * len(users_stocks))
        cursor.execute("UPDATE stocks SET available=? WHERE symbol=?", (new_available, symbol))

        self.conn.commit()

        await ctx.send(f"Distributed {amount} of {symbol} to all holders.")


    @commands.command(name="change_stock_price", help="Change the price of a specified stock.")
    @is_allowed_user(930513222820331590, PBot)
    async def change_stock_price(self, ctx, stock_symbol: str, new_price: float):
        cursor = self.conn.cursor()

        try:
            # Update the price of the stock
            cursor.execute("UPDATE stocks SET price=? WHERE symbol=?", (new_price, stock_symbol))
            self.conn.commit()

#            await ctx.send(f"The price of stock with symbol {stock_symbol} has been updated to {new_price:.10f}.")

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while updating the stock price: {str(e)}")


    @commands.command(name="merge_and_distribute", aliases=["merge"], help="Merge two stocks and distribute a specified amount of a third stock to all holders.")
    @is_allowed_user(930513222820331590)
    async def merge_and_distribute_stock(self, ctx, symbol1: str, symbol2: str, new_symbol: str, debug: bool = False):
        cursor = self.conn.cursor()

        # Check if the stocks exist.
        cursor.execute("SELECT symbol, available FROM stocks WHERE symbol=?", (symbol1,))
        stock1 = cursor.fetchone()
        cursor.execute("SELECT symbol, available FROM stocks WHERE symbol=?", (symbol2,))
        stock2 = cursor.fetchone()
        if stock1 is None or stock2 is None:
            await ctx.send(f"One or more stocks not found.")
            return


        # Get all users holding the specified stocks.
        cursor.execute("SELECT user_id, amount FROM user_stocks WHERE symbol=? OR symbol=?", (symbol1, symbol2))
        users_stocks = cursor.fetchall()

        # Combine the amounts for each user.
        combined_amounts = {}
        for user_stock in users_stocks:
            user_id, user_amount = user_stock['user_id'], user_stock['amount']
            combined_amounts[user_id] = combined_amounts.get(user_id, 0) + user_amount

            await self.remove_stock_from_user(ctx, user_id, symbol1)
            await self.remove_stock_from_user(ctx, user_id, symbol2)

        # Distribute the specified amount of the new stock to each user.
        for user_id, user_amount in combined_amounts.items():

            p3Addr = get_p3_address(self.P3addrConn, user_id)
            if p3Addr and combined_amounts[user_id] != 0:

                await ctx.send(f"User: {p3Addr} Amount: {combined_amounts[user_id]}")
                await self.give_stock(ctx, p3Addr, new_symbol, combined_amounts[user_id], False)


        await self.set_stock_supply(ctx, symbol1, 0, 0)
        await self.set_stock_supply(ctx, symbol2, 0, 0)
        symbol1_price = await get_stock_price(self, ctx, symbol1)
        symbol2_price = await get_stock_price(self, ctx, symbol2)
        new_stock_price = symbol1_price + symbol2_price
        await self.change_stock_price(ctx, symbol1, 0)
        await self.change_stock_price(ctx, symbol2, 0)
        await self.change_stock_price(ctx, new_symbol, new_stock_price)
        await ctx.send(f"Merged {symbol1} and {symbol2} into {new_symbol} and distributed to all holders.")


    @commands.command(name="send_stock", help="Send a user an amount of a stock from your stash.")
    async def send_stock_command(self, ctx, target, symbol: str, amount: int):
        sender_addr = get_p3_address(self.P3addrConn, ctx.author.id)
        await send_stock(self, ctx, target, sender_addr, symbol, amount)



    @commands.command(name="ceo_unlock")
    async def ceo_unlock(self, ctx, symbol, percentage, ceo_type):
        result = await get_supply_stats(self, ctx, symbol)
        reserve, total, locked, escrow, market, circulating = result
        amount = (int(percentage) / 100) * total

        if ceo_type.lower() == "lock":
            if int(market) < int(amount):
                await ctx.send(f"Not enough shares of {symbol} to lock\n\nMarket: {market:,.0f}\nAmount: {amount:,.0f}")
            else:
                await unlock_share_limit(self, ctx, symbol, percentage, ceo_type)
        elif ceo_type.lower() == "unlock":
            if int(locked) < int(amount):
                await ctx.send(f"Not enough shares of {symbol} to unlock\n\nLocked: {locked:,.0f}\nAmount: {amount:,.0f}")
            else:
                await unlock_share_limit(self, ctx, symbol, percentage, ceo_type)


    @commands.command(name="unlock_shares")
    @is_allowed_user(930513222820331590, PBot)
    async def unlock_shares(self, ctx, symbol, amount, type):
        total_supply, current_supply = await get_supply_info(self, ctx, symbol)

        if type.lower() == "lock":
            lock_unlock = "Locked"
            await self.sell_stock_for_bot(ctx, symbol, amount)
        elif type.lower() == "unlock":
            lock_unlock = "Unlocked"
            await self.buy_stock_for_bot(ctx, symbol, amount)


    @commands.command(name="unlock")
    async def unlock_at_metric(self, ctx, symbol, metric):
        result = await get_supply_stats(self, ctx, stock_name)
        reserve, total, locked, escrow, market, circulating = result
        total_supply = total
        current_supply = market
        total_shares_in_orders = escrow
        escrow_percentage = (total_shares_in_orders / total_supply) * 100
        if escrow_percentage <= float(metric):
            print(f"{escrow_percentage} <= {metric}")
        else:
            print(f"Nope ... {escrow_percentage} >= {metric}")




    @commands.command(name="add_stock", help="Add a new stock. Provide the stock symbol, name, price, total supply, and available amount.")
    @is_allowed_user(930513222820331590, PBot)
    async def add_stock(self, ctx, symbol: str, name: str, price: int, total_supply: int, available: int):
        cursor = self.conn.cursor()
        cursor.execute("INSERT INTO stocks (symbol, name, price, total_supply, available) VALUES (?, ?, ?, ?, ?)",
                       (symbol, name, price, total_supply, available))
        self.conn.commit()
        self.not_trading.append(symbol.lower())
        self.ipo_stocks.append(symbol.lower())
        await ctx.send(f"Added new stock: {symbol} ({name}), price: {price}, total supply: {total_supply}, available: {available}")



    @commands.command(name='stock_info', aliases=['stock_metric'], help='Show the current price, available supply, and top holders of a stock.')
    async def stock_info(self, ctx, symbol: str):
        try:
            result = await get_supply_stats(self, ctx, symbol)
            reserve, total, locked, escrow, market, circulating = result

            price = await get_stock_price(self, ctx, symbol)

            cursor = self.conn.cursor()





            # Get the top holders of the stock
            cursor.execute("""
                SELECT user_id, amount
                FROM user_stocks
                WHERE symbol = ? and amount > 0.0
                ORDER BY amount DESC
                LIMIT 10
            """, (symbol,))
            top_holders = cursor.fetchall()

            # Get the total number of holders altogether
            cursor.execute("""
                SELECT COUNT(DISTINCT user_id)
                FROM user_stocks
                WHERE symbol = ? and amount > 0.0
            """, (symbol,))
            total_holders = cursor.fetchone()[0]

            # Create an embed to display the stock information
            embed = discord.Embed(
                title=f"Stock Information for {symbol}",
                color=discord.Color.blue()
            )
            locked_percentage = (locked / total) * 100

            escrow_percentage = (escrow / total) * 100
            market_percentage = (market / total) * 100
            circulating_percentage = (circulating / total) * 100


            embed.add_field(name="Market Supply", value=f"{market:,.0f} shares\n({market_percentage:,.2f}%)", inline=False)
            embed.add_field(name="Escrow Supply", value=f"{escrow:,.0f} shares\n({escrow_percentage:,.2f}%)", inline=False)
            embed.add_field(name="Locked Supply", value=f"{locked:,.0f} shares\n({locked_percentage:,.2f}%)", inline=False)
            embed.add_field(name="Circulating Supply", value=f"{circulating:,.0f} shares\n({circulating_percentage:,.2f}%)", inline=False)
            embed.add_field(name="Total Supply", value=f"{total:,.0f} shares\n\n------------------------------------------", inline=False)


            if top_holders:
                top_holders_str = "\n".join([
                    f"{generate_crypto_address(user_id)} - {amount:,.0f}\n"
                    f"({((amount / total) * 100):,.{len(str(int((amount / total) * 100))) if ((amount / total) * 100) > 0.1 else 8}f}%)\n"
                    for user_id, amount in top_holders
                    if user_id != PBot
                ])


                embed.add_field(name="Top Holders", value=top_holders_str, inline=False)
            else:
                embed.add_field(name="Top Holders", value="No one currently holds shares.", inline=False)

            embed.add_field(name="Total Holders", value=f"{total_holders} holders", inline=False)

            await ctx.send(embed=embed)

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while fetching stock information: {e}")


# Stock Engine


    @commands.command(name="buy_stock_for_bot")
    @is_allowed_user(930513222820331590, PBot)
    async def buy_stock_for_bot(self, ctx, stock_name, amount):
        self.reserve_timer_start = timeit.default_timer()

        cursor = self.conn.cursor()

        # Get the stock information
        cursor.execute("SELECT price, available FROM stocks WHERE symbol=?", (stock_name,))
        stock_info = cursor.fetchone()

        if stock_info is None:
            await ctx.send(f"This stock '{stock_name}' does not exist.")
            return

        price, available_stock = map(Decimal, stock_info)
        total_cost = price * Decimal(amount)

        # Check if there's enough available stock
        if Decimal(amount) > available_stock:
            await ctx.send(f"Insufficient available stock for {stock_name}.")
            return

        # Update bot's stock balance and available stock
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
        await update_user_balance(self.conn, PBot, get_user_balance(self.conn, PBot) - total_cost)

        # Decay other stocks and increase price
        decay_other_stocks(self.conn, stock_name)
#        await self.increase_price_non_verbose(ctx, stock_name, float(amount))

        # Log the transaction
        await log_transaction(ledger_conn, ctx, "Buy Stock", stock_name, amount, total_cost, total_cost, 0, 0, price, "Fail")

        # Commit the transaction
        self.conn.commit()
        elapsed_time = timeit.default_timer() - self.reserve_timer_start
        self.reserve_avg.append(elapsed_time)
        avg_time = sum(self.reserve_avg) / len(self.reserve_avg)
        gas = self.calculate_tax_percentage(ctx, "sell_etf") * 100
        print(f"""
                Reserve Buy: {stock_name}
                -------------------------------------
                Price: {price:,.2f}
                Amount: {amount:,}
                Available {available_stock:,}
                -------------------------------------
                Transaction Time: {elapsed_time:.2f}
                Average Transaction Time: {avg_time:.2f}
                Current Gas: {gas:,.4f}%
        """)




    @commands.command(name="sell_stock_for_bot")
    @is_allowed_user(930513222820331590, PBot)
    async def sell_stock_for_bot(self, ctx, stock_name, amount):
        self.reserve_timer_start = timeit.default_timer()
        cursor = self.conn.cursor()

        # Get the stock information
        cursor.execute("SELECT price, available FROM stocks WHERE symbol=?", (stock_name,))
        stock_info = cursor.fetchone()

        if stock_info is None:
            await ctx.send(f"This stock '{stock_name}' does not exist.")
            return

        price, available_stock = map(Decimal, stock_info)
        total_earnings = price * Decimal(amount)

        # Update bot's stock balance and available stock
        cursor.execute("""
            UPDATE user_stocks
            SET amount = amount - ?
            WHERE user_id = ? AND symbol = ?
        """, (amount, PBot, stock_name))

        cursor.execute("""
            UPDATE stocks
            SET available = available + ?
            WHERE symbol = ?
        """, (amount, stock_name))

        # Update bot's balance
        await update_user_balance(self.conn, PBot, get_user_balance(self.conn, PBot) + total_earnings)

        # Decay other stocks and decrease price
        decay_other_stocks(self.conn, stock_name)
#        await self.decrease_price_non_verbose(ctx, stock_name, float(amount))

        # Log the transaction
        await log_transaction(ledger_conn, ctx, "Sell Stock", stock_name, amount, total_earnings, total_earnings, 0, 0, price, "Fail")

        # Commit the transaction
        self.conn.commit()
        elapsed_time = timeit.default_timer() - self.reserve_timer_start
        self.reserve_avg.append(elapsed_time)
        avg_time = sum(self.reserve_avg) / len(self.reserve_avg)
        gas = self.calculate_tax_percentage(ctx, "sell_etf") * 100
        print(f"""
                Reserve Sell: {stock_name}
                -------------------------------------
                Price: {price:,.2f}
                Amount: {amount:,}
                Available {available_stock:,}
                -------------------------------------
                Transaction Time: {elapsed_time:.2f}
                Average Transaction Time: {avg_time:.2f}
                Current Gas: {gas:,.4f}%
        """)






    @commands.command(name="reward_share_holders", help="Reward P3:Stable shareholders with additional shares.")
    @is_allowed_user(930513222820331590)
    async def reward_share_holders(self, ctx, stock_symbol: str, stakingYield: float):
        cursor = self.conn.cursor()

        # Begin the transaction
        cursor.execute("BEGIN")

        try:
            # Fetch all users who hold P3:Stable
            cursor.execute("SELECT user_id, amount FROM user_stocks WHERE symbol=? AND amount > 0", (stock_symbol,))
            stable_holders = cursor.fetchall()

            if not stable_holders:
                await ctx.send("No users hold P3:Stable.")
                return

            total_rewarded_shares = 0

            # Reward each shareholder
            for user_id, amount_held in stable_holders:
                if user_id == 1092870544988319905 or amount_held == 0.0:
                    continue

                print(f"UserID: {user_id} holds {amount_held} of {stock_symbol}")

                # Calculate the reward (stakingYield% of the held amount)
                reward_shares = int(amount_held * stakingYield)

                # Update the user's stock holdings with the rewarded shares
                new_amount_held = amount_held + reward_shares
                cursor.execute("UPDATE user_stocks SET amount=? WHERE user_id=? AND symbol=?", (new_amount_held, user_id, stock_symbol))

                total_rewarded_shares += reward_shares
                await log_stock_transfer(self, ledger_conn, ctx, "stakingYield", user_id, stock_symbol, reward_shares)
                time.sleep(0.05)

            # Commit the transaction
            self.conn.commit()

            await ctx.send(f"Reward distribution completed. Total {stock_symbol} shares rewarded: {total_rewarded_shares:,.2f} at {stakingYield * 100}%.")

        except Exception as e:
            # Rollback the transaction in case of an exception
            self.conn.rollback()
            print(f"An error occurred: {e}")
            await ctx.send("An error occurred during the reward distribution. Please try again later.")



    @commands.command(name="memeManager")
    @is_allowed_user(930513222820331590, PBot)
    async def memeManager(self, ctx, type: str, amount: int):
        cursor = self.conn.cursor()

        stableStock = "ROFLStocks"

        cursor.execute("SELECT * FROM stocks WHERE symbol=?", ("ROFLStocks",))
        stock = cursor.fetchone()


        shareLimit = 1000000000
        minPrice = 0.0000150000
        maxPrice = 0.0000350000
        priceFluctuationFactor = 0.1  # Adjust the fluctuation factor as needed

        if amount >= shareLimit:
            shares_amount = shareLimit
        else:
            shares_amount = amount

        current_price = await get_stock_price(self, ctx, stableStock)

        if type.lower() == "buy":
            # Randomly fluctuate the price of P3:Stable
            fluctuation = random.uniform(0, priceFluctuationFactor * current_price)
            if current_price == maxPrice:
                new_price = max(min(current_price + fluctuation, current_price * 2.25 + (amount / 2.25)), minPrice)
            else:
                new_price = max(min(current_price + fluctuation, maxPrice * 2.25 + (amount / 2.25)), minPrice)
            cursor.execute("""
                UPDATE stocks
                SET price = ?
                WHERE symbol = "ROFLStocks"
            """, (new_price,))
            self.conn.commit()
            print(f"Updated price to: {new_price:,.11f}")

            await ctx.send(f"{stableStock} has been bought, and its price has fluctuated to {new_price:,.11f}.")

        elif type.lower() == "sell":
            # Randomly fluctuate the price of P3:Stable
            fluctuation = random.uniform(-priceFluctuationFactor * current_price, 0)
            if current_price < maxPrice:
                new_price = max(min(current_price + fluctuation, current_price * 2.25 + (-amount / 2.75)), minPrice)
            else:
                new_price = max(min(current_price + fluctuation, maxPrice * 2.25 + (-amount / 2.25)), minPrice)
            cursor.execute("""
                UPDATE stocks
                SET price = ?
                WHERE symbol = "ROFLStocks"
            """, (new_price,))
            self.conn.commit()

            await ctx.send(f"{stableStock} has been sold, and its price has fluctuated to {new_price:,.11f}.")

        else:
            await ctx.send("Invalid operation. Use 'buy' or 'sell'.")





    @commands.command(name="stableManager")
    @is_allowed_user(930513222820331590, PBot)
    async def stableManager(self, ctx, type: str, amount: int):
        cursor = self.conn.cursor()

        user_id = ctx.author.id



        SHARE_LIMIT = 1000000000
        RESERVE_STOCKS = ["P3:Gold-Reserve", "P3:Copper-Reserve", "P3:Platinum-Reserve", "P3:Silver-Reserve"]
        STABLE_STOCK = "P3:Stable"
        MAX_PRICE_RULES = [
            (1000000000000000, 3500),
            (2000000000000000, 3250),
            (3000000000000000, 3000),
            (4000000000000000, 2750),
            (5000000000000000, 2500),
            (6000000000000000, 1750),
            (7000000000000000, 1200),
            (8000000000000000, 1000),
            (9000000000000000, 950),
            (10000000000000000, 875)
        ]


        shareLimit = min(amount, SHARE_LIMIT)

        total_buy_volume, total_sell_volume, total_volume = await calculate_volume(STABLE_STOCK, interval='daily')




        maxPrice = 875
        for volume_limit, price_limit in MAX_PRICE_RULES:
            if total_buy_volume <= volume_limit:
                maxPrice = price_limit
                break

        reserve_stock_prices = [await get_stock_price(self, ctx, stock_name) for stock_name in RESERVE_STOCKS]
        total_reserve_value = sum(price * shareLimit for price in reserve_stock_prices)

        operation_multiplier = 2 if type.lower() == "sell" else 1

        for stock_name, stock_price in zip(RESERVE_STOCKS, reserve_stock_prices):
            shares_to_operate = int(total_reserve_value / (stock_price * operation_multiplier))
            if type.lower() == "buy":
                await self.buy_stock_for_bot(ctx, stock_name, shares_to_operate)
            elif type.lower() == "sell":
                await self.sell_stock_for_bot(ctx, stock_name, shares_to_operate)

        current_stable_price = await get_stock_price(self, ctx, STABLE_STOCK)
        price_modifier = Decimal("1.0576") if type.lower() == "buy" else Decimal("0.9576")
        new_price = min(Decimal(current_stable_price) * price_modifier, Decimal(1001)) if type.lower() == "buy" else max(Decimal(current_stable_price) * price_modifier, Decimal(995))
#        new_price = min(Decimal(current_stable_price) * price_modifier, Decimal(maxPrice)) if type.lower() == "buy" else max(Decimal(current_stable_price) * price_modifier, Decimal(850))

        cursor.execute("""
            UPDATE stocks
            SET price = ?
            WHERE symbol = ?
        """, (new_price, STABLE_STOCK))
        self.conn.commit()

        is_buy = type.lower() == "buy"
        color = discord.Color.green() if is_buy else discord.Color.red()
        embed = discord.Embed(title=f"Stock Price {'Buy' if is_buy else 'Sell'} Update for {STABLE_STOCK}", color=color)
        embed.add_field(name="Old Price", value=f"{current_stable_price:,.2f} $QSE", inline=False)
        embed.add_field(name="New Price", value=f"{new_price:,.2f} $QSE", inline=False)

        await ctx.send(embed=embed)

    def check_last_action(self, action_list, user_id, current_timestamp):
        # Check if the user has made an action within the last 30 seconds
        return any(entry[0] == user_id for entry in action_list)






    @commands.command(name="blueChipBooster")
    @is_allowed_user(930513222820331590, PBot)
    async def blueChipBooster(self, ctx, type: str):
        return
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

        price_increase = random.uniform(buyPressureMin * boosterAmount, min(buyPressureMax * boosterAmount, stockMax - float(stock[2])))
        current_price = await get_stock_price(self, ctx, "BlueChipOG")
        new_price = min(current_price + price_increase, stockMax)

#        # Update the stock price
#        cursor.execute("""
#            UPDATE stocks
#            SET price = ?
#            WHERE symbol = ?
#        """, (new_price, 'BlueChipOG'))

        self.conn.commit()
        cost = boosterAmount * current_price
        metalLimSmall = 1000000000
        metalLimMedium = 10000000000
        metalLimLarge = 100000000000


#        await self.buy_stock_for_bot(ctx, "BlueChipOG", boosterAmount)
        metal_names = ["Gold", "Silver", "Copper", "Platinum"]

        for metal in metal_names:
            metal_reserve_price = await get_stock_price(self, ctx, f"P3:{metal}-Reserve")
            if metal_reserve_price >= 5000000:
                await self.sell_stock_for_bot(ctx, f"P3:{metal}-Reserve", metalLimSmall)
                pass
            elif metal_reserve_price >= 7500000:
                pass
                await self.sell_stock_for_bot(ctx, f"P3:{metal}-Reserve", metalLimMedium)
            elif metal_reserve_price >= 10000000:
                pass
                await self.sell_stock_for_bot(ctx, f"P3:{metal}-Reserve", metalLimLarge)
        if current_price >= 3500000:
            pass
#            await self.sell_stock_for_bot(ctx, "BlueChipOG", 1000000000000)

    @commands.command(name="inverseStock")
    @is_allowed_user(930513222820331590, PBot)
    async def inverseStock(self, ctx, type: str):
        return
#        for symbol in inverseStocks:
#            if type.lower() == "buy":
#                boosterAmount = 75000000
#                await self.sell_stock_for_bot(ctx, symbol, boosterAmount)
#            elif type.lower() == "sell":
#                boosterAmount = 150000000
#                await self.buy_stock_for_bot(ctx, symbol, boosterAmount)
#            else:
#                return



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



    @commands.command(name="metalReserveBooster")
    @is_allowed_user(930513222820331590, PBot)
    async def metalReserveBooster(self, ctx, metal: str, boosterAmount: float):
        if boosterAmount >= 500000000:
            boosterAmount = (boosterAmount * 0.5 * 0.25 * 0.26 * 0.5)
        else:
            boosterAmount = 25000000
        stock_name = metal
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
        reward_stock = "PokerChip"
        cursor = self.conn.cursor()
        addrDB = sqlite3.connect("P3addr.db")

        P3Addr = get_p3_address(addrDB, ctx.author.id)

        total_supply, current_supply = await get_supply_info(self, ctx, stock_name)
        reward_supply, reward_current = await get_supply_info(self, ctx, reward_stock)

        booster_amount = 50000000 if resultWin else 15000000
        supply_boost = 15000000 if resultWin else 50000000

        booster_amount = booster_amount * 2
        supply_boost = supply_boost * 2

        # Apply booster amount to total and available supply
        total_supply += supply_boost * 2
        current_supply += supply_boost * 2
        reward_supply += supply_boost * 2
        reward_current += supply_boost * 2

        # Update the stock supply
        await self.set_stock_supply(ctx, stock_name, total_supply, current_supply)
        await self.set_stock_supply(ctx, reward_stock, reward_supply, reward_current)


        reward_price = Decimal(await get_stock_price(self, ctx, reward_stock))
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
                embed.add_field(name="Current Price", value=f"{current_price:,.2f} $QSE", inline=False)
                embed.add_field(name="Total Supply", value=f"{total_supply:,.2f} shares", inline=True)
                embed.add_field(name="Available Supply", value=f"{current_supply:,.2f} shares", inline=True)

                await ledger_channel.send(embed=embed)

                embed = discord.Embed(
                    title=f"Shares Unlocked for {reward_stock}",
                    description=f"{supply_boost} shares have been unlocked for {reward_stock}.",
                    color=discord.Color.green()
                )
                embed.add_field(name="Current Price", value=f"{reward_price:,.2f} $QSE", inline=False)
                embed.add_field(name="Total Supply", value=f"{reward_supply:,.2f} shares", inline=True)
                embed.add_field(name="Available Supply", value=f"{reward_current:,.2f} shares", inline=True)

                await ledger_channel.send(embed=embed)

                cost = booster_amount * current_price
                await self.buy_stock_for_bot(ctx, stock_name, booster_amount / 2)
                await self.buy_stock_for_bot(ctx, reward_stock, booster_amount / 2)
                await self.give_stock(ctx, P3Addr, reward_stock, (booster_amount / 2), False)
                embed = discord.Embed(
                    title=f"{reward_stock} Rewards",
                    description=f"{supply_boost / 2:,} shares have been sent to {P3Addr}.",
                    color=discord.Color.purple()
                )

                await ledger_channel.send(embed=embed)

            reward_amount = (booster_amount / 2)

            return reward_amount


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
    async def increase_price(self, ctx, stock_name: str, amount: float, burn: bool = False):
        if burn:
            await update_stock_price(self, ctx, stock_name, amount, True, True, True)
        else:
            await update_stock_price(self, ctx, stock_name, amount, True)

    @commands.command(name="increase_price_non_verbose")
    @is_allowed_user(930513222820331590, PBot)
    async def increase_price_non_verbose(self, ctx, stock_name: str, amount: float):
        await update_stock_price(self, ctx, stock_name, amount, True, False)

    @commands.command(name="decrease_price")
    @is_allowed_user(930513222820331590, PBot)
    async def decrease_price(self, ctx, stock_name: str, amount: float):
        await update_stock_price(self, ctx, stock_name, amount, False)



    @commands.command(name="decrease_price_non_verbose")
    @is_allowed_user(930513222820331590, PBot)
    async def decrease_price_non_verbose(self, ctx, stock_name: str, amount: float):
        await update_stock_price(self, ctx, stock_name, amount, False, False)


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

    @commands.command(name="list_etfs", aliases=["etfs", "etps", "list_etps"], help="List all ETFs and their values.")
    async def list_etfs(self, ctx):
        self.tax_command_timer_start = timeit.default_timer()
        async with self.db_semaphore:
            async with self.transaction_lock:
                mv = await get_etf_value(self.conn, 6)
                await add_mv_metric(self, ctx, mv)
                await add_reserve_metric(self, ctx)
                cursor = self.conn.cursor()
                cursor.execute("SELECT etf_id, name FROM etfs")
                etfs = cursor.fetchall()

                if not etfs:
                    await ctx.send("No ETPs found.")
                    return

                embed = discord.Embed(title="Exchange Traded Products", description="List of all ETPs and their values:", color=discord.Color.blue())

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

                    embed.add_field(name=f"Index ID: {etf_id}", value=f"Name: {etf_name}\nValue: {etf_value:,.2f} $QSE", inline=False)

                await ctx.message.delete()
                await ctx.send(embed=embed)
                await tax_command(self, ctx)
                elapsed_time = timeit.default_timer() - self.tax_command_timer_start
                self.tax_command_avg.append(elapsed_time)
                avg_time = sum(self.tax_command_avg) / len(self.tax_command_avg)


    @commands.command(name="my_etfs", aliases=["my_etps"], help="Show the ETFs owned by the user and their quantities.")
    async def my_etfs(self, ctx):
        self.tax_command_timer_start = timeit.default_timer()
        async with self.db_semaphore:
            async with self.transaction_lock:
                user_id = ctx.author.id
                cursor = self.conn.cursor()

                await ctx.message.delete()
                cursor.execute("SELECT etf_id, quantity FROM user_etfs WHERE user_id=?", (user_id,))
                user_etfs = cursor.fetchall()

                if not user_etfs:
                    await ctx.send("You don't own any ETPs.")
                    return

                total_value = 0
                embed = discord.Embed(title="My ETPs", description="List of ETPs owned by you:", color=discord.Color.blue())

                for etf in user_etfs:
                    etf_id = etf[0]
                    quantity = etf[1]

                    cursor.execute("SELECT name FROM etfs WHERE etf_id=?", (etf_id,))
                    etf_name = cursor.fetchone()[0]

                    etf_value = await get_etf_value(self.conn, etf_id)

                    total_value += (etf_value or 0) * quantity
                    if quantity != 0.0:
                        embed.add_field(name=f"ETF ID: {etf_id}", value=f"Name: {etf_name}\nQuantity: {quantity:,}\nCurrent ETF value: {etf_value:,.2f}\nValue: {(etf_value or 0) * quantity:,.2f} $QSE", inline=False)

                embed.set_footer(text=f"Total Value: {total_value:,.2f} $QSE")
                await ctx.send(embed=embed)
                await tax_command(self, ctx)
                elapsed_time = timeit.default_timer() - self.tax_command_timer_start
                self.tax_command_avg.append(elapsed_time)
                avg_time = sum(self.tax_command_avg) / len(self.tax_command_avg)




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


    @commands.command(name="remove_stock_from_user", help="Remove a stock from a specified user's holdings.")
    @is_allowed_user(930513222820331590, PBot)
    async def remove_stock_from_user(self, ctx, user_id: int, stock_symbol: str):
        cursor = self.conn.cursor()
        p3Addr = get_p3_address(self.P3addrConn, user_id)

        try:
            # Remove the stock from user's holdings
            cursor.execute("DELETE FROM user_stocks WHERE user_id=? AND symbol=?", (user_id, stock_symbol))
            self.conn.commit()

            await ctx.send(f"Stock with symbol {stock_symbol} has been removed from user with ID {p3Addr}'s holdings.")

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while removing the stock from user's holdings: {str(e)}")



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


    async def get_etf_info(self, ctx, etf_id):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT s.symbol, s.price, s.available
            FROM stocks AS s
            INNER JOIN etf_stocks AS e ON s.symbol = e.symbol
            WHERE e.etf_id = ?
        """, (etf_id,))
        stock_data = cursor.fetchall()

        if not stock_data:
            return None

        stocks_info = []
        for stock in stock_data:
            result = await lowest_price_order(self, ctx, "sell", stock[0])
            lowest_sell_price = result["price"] if result else None
            escrow_supply = await get_total_shares_in_orders(self, stock[0])
            reserve_supply = self.get_user_stock_amount(PBot, stock[0])
            reserve_supply = reserve_supply - escrow_supply
            stocks_info.append({
                'symbol': stock[0],
                'price': stock[1],
                'available': reserve_supply,
                'lowest_sell_price': lowest_sell_price,
                'total_shares_in_escrow': escrow_supply
            })

        return stocks_info

    @commands.command(name="etf_info", aliases=["etp_info"], help="Display information about a specific ETF.")
    async def etf_info(self, ctx, etf_id: int):
        stocks_info = await self.get_etf_info(ctx, etf_id)

        if not stocks_info:
            await ctx.send(f"No stocks found for ETF ID: {etf_id}.")
            return

        etf_price = sum(stock['price'] for stock in stocks_info)

        chunk_size = 10
        stock_chunks = list(chunk_list(stocks_info, chunk_size))

        embeds = []
        for i, stock_chunk in enumerate(stock_chunks, start=1):
            embed = discord.Embed(title=f"ETP Information (Index ID: {etf_id})", color=discord.Color.blue())
            embed.add_field(name="Asset", value="Price ($QSE)\nAvailable Supply\nTotal Shares in Escrow\nOrder Price", inline=False)
            for stock in stock_chunk:
                symbol = stock['symbol']
                price = stock['price']
                available = stock['available']
                total_shares_in_escrow = stock['total_shares_in_escrow']
                lowest_sell_price = stock['lowest_sell_price']


                embed.add_field(name=symbol, value=f"{price:,.2f} $QSE\n{int(available):,.0f} shares\n{total_shares_in_escrow:,.0f} shares\n{lowest_sell_price:,.2f} $QSE" if lowest_sell_price else f"{price:,.2f} $QSE\n{int(available):,.0f} shares\n{total_shares_in_escrow:,.0f} shares", inline=False)

            if len(stock_chunks) > 1:
                embed.set_footer(text=f"Page {i}/{len(stock_chunks)}")
            embeds.append(embed)

        total_etf_value = etf_price
        embeds[-1].add_field(name="Total ETP Value", value=f"${total_etf_value:,.2f}", inline=False)

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
                except asyncio.TimeoutError:
                    await ctx.send("Pagination timeout.")
                    break

                if str(reaction.emoji) == "▶️" and current_page < len(embeds) - 1:
                    current_page += 1
                elif str(reaction.emoji) == "◀️" and current_page > 0:
                    current_page -= 1

                await message.edit(embed=embeds[current_page])

                try:
                    await message.remove_reaction(reaction, user)
                except discord.errors.NotFound:
                    pass

                try:
                    await ctx.message.delete()
                except discord.errors.NotFound:
                    pass

        await message.clear_reactions()


    async def send_percentage_change_embed(self, ctx, interval, percentage_change):
        # Create an embed for the percentage change message
        embed = discord.Embed(
            title=f"{interval} Percentage Change",
            description=f"{percentage_change:,.2f}%",
            color=discord.Color.green()
        )

        # Send the embed message to the channel
        await ctx.send(embed=embed)



    @commands.command(name="market_polling", help="Update the name of the ETF 6 voice channel with its value.")
    async def update_etf_value(self, ctx):
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
        mv = await get_etf_value(self.conn, 6)
        await add_mv_metric(self, ctx, mv)
        await add_reserve_metric(self, ctx)
        mv_avg = await calculate_average_mv(self)
        print(f"\n\n\nMV Avg: {mv_avg:,.2f}\nCMV: {mv:,.2f}\n\n\n")
        while True:

            etf_6_value = await get_etf_value(self.conn, 6)
            etf_6_value_formatted = locale.format_string("%0.2f", etf_6_value, grouping=True)

            voice_channel_id = 1161706930981589094  # Replace with the actual ID
            voice_channel = ctx.guild.get_channel(voice_channel_id)
            gas_channel_id = 1182436005530320917
            gas_channel = ctx.guild.get_channel(gas_channel_id)

            if gas_channel:
                current_name = gas_channel.name
                old_gas_fee_match = re.search(r"Gas: ([0-9,.]+)%", current_name)

                if old_gas_fee_match:
                    old_gas_fee = float(old_gas_fee_match.group(1))
                else:
                    old_gas_fee = 0.0

                new_gas_fee = self.calculate_tax_percentage(ctx, "sell_etf") * 100
                percentage_change = new_gas_fee - old_gas_fee

                if new_gas_fee > old_gas_fee:
                    direction_arrow = "↗"
                elif new_gas_fee < old_gas_fee:
                    direction_arrow = "↘"
                else:
                    if abs(new_gas_fee - old_gas_fee) < 0.01:
                        direction_arrow = "→"

                if new_gas_fee >= 15:
                    color = f"🔴 {direction_arrow}"
                elif new_gas_fee >= 10:
                    color = f"🟡 {direction_arrow}"
                else:
                    color = f"🟢 {direction_arrow}"

                new_name = f"Gas: {new_gas_fee:,.2f}% {color} {percentage_change:+,.2f}%"
                await gas_channel.edit(name=new_name)


            if voice_channel:
                old_name = voice_channel.name
                old_price_match = re.search(r"MV: ([\d,]+\.\d+)", old_name)

                if old_price_match:
                    old_price_str = old_price_match.group(1).replace(",", "")
                    try:
                        old_price = float(old_price_str)
                    except ValueError:
                        old_price = None
                else:
                    old_price = None

                if old_price is not None:
                    percentage_change = ((etf_6_value - old_price) / old_price) * 100
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
                    embed.add_field(
                        name="Gas Fee",
                        value=f"{new_gas_fee}% {direction_arrow}",
                        inline=False
                    )
                    color = f"🟡 →" if abs(percentage_change) < 0.000001 else f"📈 ↗" if percentage_change > 0 else f"📉 ↘"
                    new_name = f"MV: {etf_6_value_formatted} {color}"
                    await voice_channel.edit(name=new_name)
                    await ctx.send(embed=embed)
                else:
                    await ctx.send("Failed to retrieve the old price from the channel name.")
            else:
                await ctx.send(f"Voice channel with ID '{voice_channel_id}' not found.")



            self.run_counter += 1
            gas = self.calculate_tax_percentage(ctx, "sell_etf") * 100
            print(f"Gas Debug: {gas}%\nCounter: {self.run_counter}\nMarket: {etf_6_value_formatted}({percentage_change:,.5f}%)\nTrading Halted: {self.is_halted}\nHalted Stocks: {self.not_trading}\nStocks in maintenance: {self.maintenance}\nStocks in IPO: {self.ipo_stocks}")

            if self.run_counter == 2016:
                self.buy_stock_avg = [1]
                self.sell_stock_avg = [1]
                self.buy_etf_avg = [1]
                self.sell_etf_avg = [1]
                self.casino_avg = [1]
                self.run_counter = 0
            await asyncio.sleep(300)


# Buy/Sell ETFs
    @commands.command(name="buy_etf", help="Buy an ETF. Provide the ETF ID and quantity.")
    @is_allowed_user(930513222820331590, PBot)
    async def buy_etf(self, ctx, etf_id: int, quantity: int):
        await ctx.message.delete()
        current_timestamp = datetime.utcnow()
        user_id = ctx.author.id
        P3addrConn = sqlite3.connect("P3addr.db")
        P3addr = get_p3_address(P3addrConn, user_id)
        # Fetch the ETF's value and calculate the total cost
        etf_value = await get_etf_value(self.conn, etf_id)

        color = discord.Color.green()
        embed = discord.Embed(title=f"ETP Transaction Processing", color=color)
        embed.add_field(name="Address:", value=f"{P3addr}", inline=False)
        embed.add_field(name="ETF:", value=f"{etf_id}", inline=False)
        embed.add_field(name="Amount:", value=f"{quantity:,.2f}", inline=False)
        embed.add_field(name="Value:", value=f"{etf_value:,.2f} $QSE", inline=False)
        embed.set_footer(text=f"Timestamp: {current_timestamp}")

        await ctx.send(embed=embed)



        cursor = self.conn.cursor()


        current_timestamp = datetime.utcnow()
        self.last_buyers = [entry for entry in self.last_buyers if (current_timestamp - entry[1]).total_seconds() <= self.calculate_average_time_type("buy_etf")]
        if self.check_last_action(self.last_buyers, user_id, current_timestamp):
#            await ctx.send(f"You can't make another buy within {self.calculate_average_time_type("buy_etf")} seconds of your last action.")
            return
        # Add the current action to the list
        self.last_buyers.append((user_id, current_timestamp))


        # Check if user already holds the maximum allowed quantity of the ETF
        cursor.execute("SELECT COALESCE(SUM(quantity), 0) FROM user_etfs WHERE user_id=? AND etf_id=?", (user_id, etf_id))
        current_holding = cursor.fetchone()[0]



#        if int(current_holding) + int(quantity) >= int(dETFLimit + 1):
#            await ctx.send("Reached Max ETP Limit")
#            return

        # Fetch the ETF's value and calculate the total cost
        etf_value = await get_etf_value(self.conn, etf_id)

        total_cost = Decimal(etf_value) * Decimal(quantity)


        # Calculate the tax amount based on dynamic factors
        tax_percentage = self.calculate_tax_percentage(ctx, "buy_etf")
        fee = total_cost * Decimal(tax_percentage)
        total_cost_with_tax = total_cost + fee

        # Check if user has enough balance to buy the ETF
        current_balance = get_user_balance(self.conn, user_id)

        if total_cost_with_tax > current_balance:
            # Calculate the missing amount needed to complete the transaction including tax.
            missing_amount = total_cost - current_balance
            await ctx.send(f"{ctx.author.mention}, you do not have enough $QSE to buy this ETP. You need {missing_amount:,.2f} more $QSE, including Gas, to complete this purchase.")
            return

        new_balance = current_balance - total_cost_with_tax

        try:
            P3addrConn = sqlite3.connect("P3addr.db")
            PBotAddr = get_p3_address(P3addrConn, PBot)
            await self.give_addr(ctx, PBotAddr, int(total_cost), False)
            user_id = ctx.author.id
            add_city_tax(user_id, fee)
#            update_user_balance(self.conn, user_id, new_balance)
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
        # Transfer the tax amount to the bot's address P3:03da907038
#        update_user_balance(self.conn, get_user_id(self.P3addrConn, self.bot_address), get_user_balance(self.conn, get_user_id(self.P3addrConn, self.bot_address)) + fee)
        await log_transfer(self, ledger_conn, ctx, "P3 Bot", self.bot_address, get_user_id(self.P3addrConn, self.bot_address), fee)
        decay_other_stocks(self.conn, "P3:BANK")
        await log_transaction(ledger_conn, ctx, "Buy ETP", str(etf_id), quantity, total_cost, total_cost_with_tax, current_balance, new_balance, etf_value, "True")
#        await self.blueChipBooster(ctx, "BUY")
        await self.inverseStock(ctx, "buy")
        self.conn.commit()
        if etf_id == 10:
            await self.whaleBooster(ctx)
        if etf_id == 3:
            await self.sludgeBoost(ctx, quantity)
        elapsed_time = timeit.default_timer() - self.buy_etf_timer_start
        self.buy_etf_avg.append(elapsed_time)
        avg_time = sum(self.buy_etf_avg) / len(self.buy_etf_avg)
        user_id = ctx.author.id
        P3addrConn = sqlite3.connect("P3addr.db")
        P3addr = get_p3_address(P3addrConn, user_id)
        current_timestamp = datetime.utcnow()
        color = discord.Color.green()
        embed = discord.Embed(title=f"ETP Transaction Completed", color=color)
        embed.add_field(name="Address:", value=f"{P3addr}", inline=False)
        embed.add_field(name="ETP:", value=f"{etf_id}", inline=False)
        embed.add_field(name="Amount:", value=f"{quantity:,.2f}", inline=False)
        embed.add_field(name="Total Cost w/Gas:", value=f"{total_cost_with_tax:,.2f} $QSE", inline=False)
        embed.add_field(name="Old Balance:", value=f"{current_balance:,.2f} $QSE", inline=False)
        embed.add_field(name="New Balance:", value=f"{new_balance:,.2f} $QSE", inline=False)
        tax_rate = tax_percentage * 100
        embed.add_field(name="Gas Rate:", value=f"{tax_rate:.2f}%", inline=False)
        embed.add_field(name="Transaction Time:", value=f"{elapsed_time:.2f} seconds", inline=False)
        embed.set_footer(text=f"Timestamp: {current_timestamp}")
        await add_experience(self, self.conn, ctx.author.id, 3, ctx)
        await ctx.send(embed=embed)
        print(f"""
                **ETP_DEBUG**
                -------------------------------------
                Index ID: {etf_id}
                Action: Buy
                User: {generate_crypto_address(user_id)}
                Amount: {quantity:,}
                Price: {etf_value:,.2f} $QSE
                Gas Paid: {fee:,.2f} $QSE
                Transaction Time: {elapsed_time:.2f}
                Average Transaction Time: {avg_time:.2f}
                Gas Fee: {(self.calculate_tax_percentage(ctx, "buy_etf") * 100):.2f}%
                Timestamp: {current_timestamp}
                -------------------------------------
            """)





    @commands.command(name="sell_etf", help="Sell an ETF. Provide the ETF ID and quantity.")
    @is_allowed_user(930513222820331590, PBot)
    async def sell_etf(self, ctx, etf_id: int, quantity: int):
        await ctx.message.delete()
        current_timestamp = datetime.utcnow()
        user_id = ctx.author.id
        P3addrConn = sqlite3.connect("P3addr.db")
        P3addr = get_p3_address(P3addrConn, user_id)
        # Fetch the ETF's value and calculate the total cost
        etf_value = await get_etf_value(self.conn, etf_id)

        color = discord.Color.red()
        embed = discord.Embed(title=f"ETP Transaction Processing", color=color)
        embed.add_field(name="Address:", value=f"{P3addr}", inline=False)
        embed.add_field(name="ETP:", value=f"{etf_id}", inline=False)
        embed.add_field(name="Amount:", value=f"{quantity:,.2f}", inline=False)
        embed.add_field(name="Value:", value=f"{etf_value:,.2f} $QSE", inline=False)
        embed.set_footer(text=f"Timestamp: {current_timestamp}")

        await ctx.send(embed=embed)
        member = ctx.guild.get_member(user_id)
        cursor = self.conn.cursor()


        current_timestamp = datetime.utcnow()
        self.last_sellers = [entry for entry in self.last_sellers if (current_timestamp - entry[1]).total_seconds() <= self.calculate_average_time_type("sell_etf")]
        if self.check_last_action(self.last_sellers, user_id, current_timestamp):
#            await ctx.send(f"You can't make another sell within {self.calculate_average_time_type("sell_etf")} seconds of your last action.")
            return
            # Add the current action to the list
        self.last_sellers.append((user_id, current_timestamp))

        # Check if user holds the specified quantity of the ETF
        cursor.execute("SELECT quantity FROM user_etfs WHERE user_id=? AND etf_id=?", (user_id, etf_id))
        current_holding = cursor.fetchone()

        if current_holding is None or current_holding[0] < quantity:
            await ctx.send("Insufficient ETP holdings.")
            return

        # Fetch the ETF's value and calculate the total cost
        etf_value = await get_etf_value(self.conn, etf_id)

        total_sale_amount = Decimal(etf_value) * Decimal(quantity)

        # Calculate the tax amount based on dynamic factors
        tax_percentage = self.calculate_tax_percentage(ctx, "sell_etf")  # Custom function to determine the tax percentage based on quantity and total_sale_amount

        fee = total_sale_amount * Decimal(tax_percentage)
        total_sale_amount_with_tax = total_sale_amount - fee

        # Update user's balance
        current_balance = get_user_balance(self.conn, user_id)
        new_balance = current_balance + total_sale_amount_with_tax

        try:
            await self.send_from_reserve(ctx, user_id, int(total_sale_amount_with_tax))
            user_id = ctx.author.id
            add_city_tax(user_id, fee)
#            update_user_balance(self.conn, user_id, new_balance)
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
            await ctx.send(f"An error occurred while updating user ETP holdings. Error: {str(e)}")
            return
        # Transfer the tax amount to the bot's address P3:03da907038
#        update_user_balance(self.conn, get_user_id(self.P3addrConn, self.bot_address), get_user_balance(self.conn, get_user_id(self.P3addrConn, self.bot_address)) + fee)
        await log_transfer(self, ledger_conn, ctx, "P3 Bot", self.bot_address, get_user_id(self.P3addrConn, self.bot_address), fee)
        decay_other_stocks(self.conn, "P3:BANK")
        await log_transaction(ledger_conn, ctx, "Sell ETP", str(etf_id), quantity, total_sale_amount, total_sale_amount_with_tax, current_balance, new_balance, etf_value, "True")
#        await self.blueChipBooster(ctx, "SELL")
        await self.inverseStock(ctx, "sell")
        self.conn.commit()
        if etf_id == 10:
            await self.whaleBooster(ctx)
        elapsed_time = timeit.default_timer() - self.sell_etf_timer_start
        self.sell_etf_avg.append(elapsed_time)
        avg_time = sum(self.sell_etf_avg) / len(self.sell_etf_avg)
        user_id = ctx.author.id
        P3addrConn = sqlite3.connect("P3addr.db")
        P3addr = get_p3_address(P3addrConn, user_id)
        current_timestamp = datetime.utcnow()
        color = discord.Color.red()
        embed = discord.Embed(title=f"ETP Transaction Completed", color=color)
        embed.add_field(name="Address:", value=f"{P3addr}", inline=False)
        embed.add_field(name="ETP:", value=f"{etf_id}", inline=False)
        embed.add_field(name="Amount:", value=f"{quantity:,.2f}", inline=False)
        embed.add_field(name="Total Sale w/Gas:", value=f"{total_sale_amount_with_tax:,.2f} $QSE", inline=False)
        embed.add_field(name="Old Balance:", value=f"{current_balance:,.2f} $QSE", inline=False)
        embed.add_field(name="New Balance:", value=f"{new_balance:,.2f} $QSE", inline=False)
        tax_rate = tax_percentage * 100
        embed.add_field(name="Gas Rate:", value=f"{tax_rate:.2f}%", inline=False)
        embed.add_field(name="Transaction Time:", value=f"{elapsed_time:.2f} seconds", inline=False)
        embed.set_footer(text=f"Timestamp: {current_timestamp}")
        await add_experience(self, self.conn, ctx.author.id, 3, ctx)


        await ctx.send(embed=embed)
        print(f"""
                    **ETP_DEBUG**
                --------------------
                Index ID: {etf_id}
                Action: Sell
                User: {generate_crypto_address(user_id)}
                Amount: {quantity:,}
                Price: {etf_value:,.2f} $QSE
                Gas Paid: {fee:,.2f} $QSE
                Transaction Time: {elapsed_time:.2f}
                Average Transaction Time: {avg_time:.2f}
                Gas Fee: {(self.calculate_tax_percentage(ctx, "sell_etf") * 100):.2f}%
                Timestamp: {current_timestamp}
                --------------------
            """)



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
            await ctx.send(f"{ctx.author.mention}, you don't have enough $QSE to buy {quantity} tickets. You need {cost} $QSE.")
            return

        # Calculate the tax amount based on dynamic factors
        tax_percentage = self.calculate_tax_percentage(ctx, "buy_etf") # Custom function to determine the tax percentage based on quantity and cost
        fee = cost * Decimal(tax_percentage)
        total_cost = cost + fee


        # Check if the user will have a negative balance after buying the tickets
        if user_balance - total_cost < 0:
            await ctx.send(f"{ctx.author.mention}, this transaction will result in a negative balance. Please buy a lower quantity.")
            return

        # Deduct the cost of tickets from the user's balance
        await update_user_balance(self.conn, user_id, user_balance - total_cost)

        ticket_data = await get_ticket_data(self.conn, user_id)

        if ticket_data is None:
            await update_ticket_data(self.conn, user_id, quantity, int(time.time()))
        else:
            ticket_quantity, _ = ticket_data
            await update_ticket_data(self.conn, user_id, ticket_quantity + quantity, int(time.time()))

        # Transfer the tax amount to the bot's address P3:03da907038
        await update_user_balance(self.conn, get_user_id(self.P3addrConn, self.bot_address), get_user_balance(self.conn, get_user_id(self.P3addrConn, self.bot_address)) + fee)
        await log_transfer(self, ledger_conn, ctx, "P3 Bot", self.bot_address, get_user_id(self.P3addrConn, self.bot_address), fee)
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
            await ctx.send(f"{ctx.author.mention}, you have {ticket_quantity:,.0f} tickets.")



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

            embed.add_field(name=item_name, value=f"Description: {item_description}\nPrice: {item_price} $QSE\nUsable: {'Yes' if is_usable else 'No'}", inline=False)

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
            quantity = Decimal(item[2])
            item_price = Decimal(item[3])  # Convert the item price to Decimal

            # Calculate the total value for the item
            item_value = item_price * quantity

            total_value += item_value  # Accumulate the total value

            # Format the values with commas
            formatted_quantity = "{:,}".format(quantity)
            formatted_item_value = "{:,.2f}".format(item_value)

            embed.add_field(name=item_name, value=f"Description: {item_description}\nQuantity: {formatted_quantity}\nValue: {formatted_item_value} $QSE", inline=False)

        # Format the total inventory value with commas
        formatted_total_value = "{:,.2f}".format(total_value)

        # Add the total value of the inventory to the embed
        embed.add_field(name="Total Inventory Value", value=f"{formatted_total_value} $QSE", inline=False)

        await ctx.send(embed=embed)

    @commands.command(name="initialize_dates", help="Initialize all users' dates to the default.")
    async def initialize_dates(self, ctx):
        try:
            with self.get_cursor() as cursor:
                cursor.execute("UPDATE item_usage SET timestamp = '2000-01-01 00:00:00'")
            await ctx.send("All users' dates have been initialized to the default.")
        except sqlite3.Error as e:
            await ctx.send(f"An error occurred: {str(e)}")





    @commands.command(name="buy_item", help="Buy an item from the marketplace.")
    @is_allowed_user(930513222820331590, PBot)
    async def buy_item(self, ctx, item_name: str, quantity: int):
        await ctx.message.delete()
        current_timestamp = datetime.utcnow()
        user_id = ctx.author.id
        P3addrConn = sqlite3.connect("P3addr.db")
        P3addr = get_p3_address(P3addrConn, user_id)

        color = discord.Color.green()
        embed = discord.Embed(title=f"Item Transaction Processing", color=color)
        embed.add_field(name="Address:", value=f"{P3addr}", inline=False)
        embed.add_field(name="Item:", value=f"{item_name}", inline=False)
        embed.add_field(name="Amount:", value=f"{quantity:,.2f}", inline=False)
#        embed.add_field(name="Value:", value=f"{etf_value:,.2f} $QSE", inline=False)
        embed.set_footer(text=f"Timestamp: {current_timestamp}")

        await ctx.send(embed=embed)
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
        tax_percentage = self.calculate_tax_percentage(ctx, "buy_etf")  # Custom function to determine the tax percentage based on quantity and cost
        tax_amount = Decimal(total_cost) * Decimal(tax_percentage)

        # Check if the user has enough balance to buy the items
        current_balance = get_user_balance(self.conn, user_id)

        if total_cost + tax_amount > current_balance:
            await ctx.send(f"{ctx.author.mention}, you do not have enough $QSE to buy these items.")
            return

        new_balance = current_balance - (total_cost + tax_amount)

        # Update the user's balance
        try:
            P3addrConn = sqlite3.connect("P3addr.db")
            PBotAddr = get_p3_address(P3addrConn, PBot)
            await self.give_addr(ctx, PBotAddr, (int(total_cost) + int(tax_amount)), False)
#            update_user_balance(self.conn, user_id, new_balance)
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
        # Transfer the tax amount to the bot's address P3:03da907038
#        update_user_balance(self.conn, get_user_id(self.P3addrConn, self.bot_address), get_user_balance(self.conn, get_user_id(self.P3addrConn, self.bot_address)) + tax_amount)
        await log_transfer(self, ledger_conn, ctx, "P3 Bot", self.bot_address, get_user_id(self.P3addrConn, self.bot_address), tax_amount)
        await log_item_transaction(self.conn, ctx, "Buy", item_name, quantity, total_cost, tax_amount, new_balance)
        await self.inverseStock(ctx, "buy")
        self.conn.commit()


        # Calculate the amount to buy of the metal stock
        amount_to_buy = total_cost  # Adjust this if needed based on your specific requirements

        if item_name in ["Gold", "Copper", "Silver", "Platinum"]:
            stock_symbol = f"P3:{item_name}-Reserve"
            stock_price = Decimal(await get_stock_price(self, ctx, stock_symbol))  # Explicitly convert to Decimal
            shares_to_buy = int(amount_to_buy / stock_price)
            await self.metalReserveBooster(ctx, stock_symbol, shares_to_buy)
            new_price = Decimal(await get_stock_price(self, ctx, stock_symbol))

            current_timestamp = datetime.utcnow()
            user_id = ctx.author.id
            P3addrConn = sqlite3.connect("P3addr.db")
            P3addr = get_p3_address(P3addrConn, user_id)

            color = discord.Color.green()
            embed = discord.Embed(title=f"Item Transaction Completed", color=color)
            embed.add_field(name="Address:", value=f"{P3addr}", inline=False)
            embed.add_field(name="Item:", value=f"{item_name}", inline=False)
            embed.add_field(name="Amount:", value=f"{quantity:,.2f}", inline=False)
            embed.add_field(name="Value:", value=f"{total_cost:,.2f} $QSE", inline=False)
            embed.add_field(name="Gas:", value=f"{tax_amount:,.2f} $QSE", inline=False)
            embed.add_field(name="Reserve Stock:", value=f"{stock_symbol}", inline=False)
            embed.add_field(name="Shares purchased by Reserve:", value=f"{shares_to_buy:,}", inline=False)
            embed.set_footer(text=f"Timestamp: {current_timestamp}")

            await ctx.send(embed=embed)


            elapsed_time = timeit.default_timer() - self.buy_item_timer_start
            self.buy_item_avg.append(elapsed_time)
            avg_time = sum(self.buy_item_avg) / len(self.buy_item_avg)
            gas = self.calculate_tax_percentage(ctx, "sell_etf") * 100

            print(f"""
            **Metal Reserve Debug**
            Metal: {item_name}
            Price: {item_price:,.2f}
            Quantity: {quantity:,}
            Reserve: {stock_symbol}
            Shares to buy: {shares_to_buy:,}
            Current Price: {stock_price:,.2f}
            New Price: {new_price:,.2f}
            Transaction Time: {elapsed_time:.2f} seconds
            Average Time: {avg_time:.2f} seconds
            Gas Fee: {gas:,.4f}

            """)

    @commands.command(name="sell_item", help="Sell an item from your inventory.")
    @is_allowed_user(930513222820331590, PBot)
    async def sell_item(self, ctx, item_name: str, quantity: int):
        await ctx.message.delete()
        current_timestamp = datetime.utcnow()
        user_id = ctx.author.id
        P3addrConn = sqlite3.connect("P3addr.db")
        P3addr = get_p3_address(P3addrConn, user_id)

        color = discord.Color.red()
        embed = discord.Embed(title=f"Item Transaction Processing", color=color)
        embed.add_field(name="Address:", value=f"{P3addr}", inline=False)
        embed.add_field(name="Item:", value=f"{item_name}", inline=False)
        embed.add_field(name="Amount:", value=f"{quantity:,.2f}", inline=False)
#        embed.add_field(name="Value:", value=f"{etf_value:,.2f} $QSE", inline=False)
        embed.set_footer(text=f"Timestamp: {current_timestamp}")

        await ctx.send(embed=embed)
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
        tax_percentage = self.calculate_tax_percentage(ctx, "sell_etf")  # Custom function to determine the tax percentage based on quantity and cost
        fee = total_sale_amount * Decimal(tax_percentage)
        total_sale_amount = total_sale_amount - fee
        total_cost = total_sale_amount
        tax_amount = fee

        # Update user's balance
        current_balance = get_user_balance(self.conn, user_id)
        new_balance = current_balance + total_sale_amount

        try:
            await self.send_from_reserve(ctx, user_id, total_sale_amount)
#            update_user_balance(self.conn, user_id, new_balance)
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
        # Transfer the tax amount to the bot's address P3:03da907038
#        update_user_balance(self.conn, get_user_id(self.P3addrConn, self.bot_address), get_user_balance(self.conn, get_user_id(self.P3addrConn, self.bot_address)) + tax_amount)
        await log_transfer(self, ledger_conn, ctx, "P3 Bot", self.bot_address, get_user_id(self.P3addrConn, self.bot_address), tax_amount)
        await log_item_transaction(self.conn, ctx, "Sell", item_name, quantity, total_sale_amount, Decimal(fee), new_balance)
        await self.inverseStock(ctx, "sell")
        self.conn.commit()

        current_timestamp = datetime.utcnow()
        user_id = ctx.author.id
        P3addrConn = sqlite3.connect("P3addr.db")
        P3addr = get_p3_address(P3addrConn, user_id)

        color = discord.Color.red()
        embed = discord.Embed(title=f"Item Transaction Completed", color=color)
        embed.add_field(name="Address:", value=f"{P3addr}", inline=False)
        embed.add_field(name="Item:", value=f"{item_name}", inline=False)
        embed.add_field(name="Amount:", value=f"{quantity:,.2f}", inline=False)
        embed.add_field(name="Value:", value=f"{total_cost:,.2f} $QSE", inline=False)
        embed.add_field(name="Gas:", value=f"{tax_amount:,.2f} $QSE", inline=False)
        embed.set_footer(text=f"Timestamp: {current_timestamp}")

        await ctx.send(embed=embed)
        elapsed_time = timeit.default_timer() - self.sell_item_timer_start
        self.sell_item_avg.append(elapsed_time)
        avg_time = sum(self.sell_item_avg) / len(self.sell_item_avg)


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
            await ctx.send(f"The balance for user with ID {user_id} has been adjusted to {new_balance:,.2f} $QSE.")
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





    @commands.command(name="mint_stock", help="Set the total and available supply for a specific stock.")
    @is_allowed_user(930513222820331590, PBot)
    async def mint_stock_supply(self, ctx, stock_name: str, mint: int, verbose: bool = True):
        cursor = self.conn.cursor()

        try:
            # Check if the stock exists
            cursor.execute("SELECT * FROM stocks WHERE symbol=?", (stock_name,))
            stock = cursor.fetchone()
            if stock is None:
                await ctx.send(f"This stock '{stock_name}' does not exist.")
                return

            result = await get_supply_stats(self, ctx, stock_name)
            reserve, total, locked, escrow, market, circulating = result
            new_reserve = reserve + mint
            new_total = total + mint

            if total is not None:
                if total < 0:
                    await ctx.send("Invalid total supply value. The total supply must be non-negative.")
                    return
                cursor.execute("UPDATE stocks SET total_supply = ? WHERE symbol = ?", (new_total, stock_name))

            if reserve is not None:
                if reserve < 0:
                    await ctx.send("Invalid available supply value. The available supply must be non-negative.")
                    return
                cursor.execute("UPDATE stocks SET available = ? WHERE symbol = ?", (new_reserve, stock_name))

            self.conn.commit()
            if verbose:
            	await ctx.send(f"Minted {mint:,.0f} to {stock_name}\n\n{reserve:,.0f}->{new_reserve:,.0f}\n{total:,.0f}->{new_total:,.0f}")

        except sqlite3.Error as e:
            await ctx.send(f"An error occurred while updating the stock supply. Error: {str(e)}")


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






    @commands.command(name='roulette', help='Play roulette. Choose a color (red/black/green) or a number (0-36) or "even"/"odd" and your bet amount.')
    async def roulette(self, ctx, choice: str, bet: int):
        self.casino_timer_start = timeit.default_timer()
        user_id = ctx.author.id
        current_balance = get_user_balance(self.conn, user_id)
        current_timestamp = datetime.utcnow()
        self.last_gamble = [entry for entry in self.last_gamble if (current_timestamp - entry[1]).total_seconds() <= 5]


        if self.check_last_action(self.last_gamble, user_id, current_timestamp):
            await ctx.send("You can't make another play within 5 seconds of your last action.")
            return
        # Add the current action to the list
        self.last_gamble.append((user_id, current_timestamp))


        # Check if bet amount is positive
        if bet <= 0:
            await ctx.send(f"{ctx.author.mention}, bet amount must be a positive number.")

        if bet < minBet:
            await ctx.send(f"{ctx.author.mention}, minimum bet is {minBet:,.2f} $QSE.")
            return

        if bet > maxBet:
            await ctx.send(f"{ctx.author.mention}, the maximum bet amount is {maxBet:,.2f} $QSE.")
            return
        if bet > current_balance:
            # Calculate the missing amount needed to complete the transaction including tax.
            missing_amount = bet - current_balance
            await ctx.send(f"{ctx.author.mention}, you do not have enough $QSE to place the bet. You need {missing_amount:,.2f} more $QSE, including Gas, to place this bet.")
            return

        # Initialize roulette wheel as numbers 0-36 with their associated colors
        red_numbers = list(range(1, 11, 2)) + list(range(12, 19, 2)) + list(range(19, 29, 2)) + list(range(30, 37, 2))
        wheel = [(str(i), 'red' if i in red_numbers else 'black' if i != 0 else 'green') for i in range(37)]

        # Spin the wheel
        spin_result, spin_color = random.choice(wheel)

        tax_percentage = self.calculate_tax_percentage(ctx, "sell_etf")
        tax = (bet * Decimal(tax_percentage))
        total_cost = bet + tax

        # Check for negative balance after tax
        if current_balance - total_cost < 0:
            await ctx.send(f"{ctx.author.mention}, you don't have enough $QSE to cover the bet and Gas.")
            return

        # Deduct bet and tax from user's current balance
        new_balance = current_balance - total_cost
        await update_user_balance(self.conn, user_id, new_balance)

        # Check if the user wins
        win_amount = 0
        if choice.lower() == spin_color:
            win_amount = bet * 4  # Payout for color choice is 2x
        elif choice.isdigit() and int(choice) == int(spin_result):
            win_amount = bet * 70  # Payout for number choice is 35x
        elif choice.lower() == "even" and int(spin_result) % 2 == 0 and spin_result != '0':
            win_amount = bet * 4  # Payout for 'even' is 2x
        elif choice.lower() == "odd" and int(spin_result) % 2 != 0:
            win_amount = bet * 4  # Payout for 'odd' is 2x

        if win_amount > 0:
            new_balance += win_amount
            await update_user_balance(self.conn, user_id, new_balance)
            await ctx.send(f"{ctx.author.mention}, congratulations! The ball landed on {spin_result} ({spin_color}). You won {win_amount} $QSE. Your new balance is {new_balance:,.2f}.")
            await log_gambling_transaction(ledger_conn, ctx, "Roulette", bet, f"You won {win_amount} $QSE", new_balance)
            await self.casinoTool(ctx, True)
        else:
            await ctx.send(f"{ctx.author.mention}, the ball landed on {spin_result} ({spin_color}). You lost {total_cost} $QSE including Gas. Your new balance is {new_balance:,.2f}.")
            await log_gambling_transaction(ledger_conn, ctx, "Roulette", bet, f"You lost {total_cost} $QSE", new_balance)
            await self.casinoTool(ctx, False)
        # Transfer the tax amount to the bot's address P3:03da907038
        await update_user_balance(self.conn, get_user_id(self.P3addrConn, self.bot_address), get_user_balance(self.conn, get_user_id(self.P3addrConn, self.bot_address)) + tax)
        await log_transfer(self, ledger_conn, ctx, "P3 Bot", self.bot_address, get_user_id(self.P3addrConn, self.bot_address), tax)
        elapsed_time = timeit.default_timer() - self.casino_timer_start
        self.casino_avg.append(elapsed_time)

    @commands.command(name='coinflip', help='Flip a coin and bet on heads or tails.')
    async def coinflip(self, ctx, choice: str, bet: int):
        self.casino_timer_start = timeit.default_timer()
        user_id = ctx.author.id
        current_balance = get_user_balance(self.conn, user_id)
        current_timestamp = datetime.utcnow()
        self.last_gamble = [entry for entry in self.last_gamble if (current_timestamp - entry[1]).total_seconds() <= 5]


        if self.check_last_action(self.last_gamble, user_id, current_timestamp):
            await ctx.send("You can't make another play within 5 seconds of your last action.")
            return
        # Add the current action to the list
        self.last_gamble.append((user_id, current_timestamp))

        # Check if bet amount is positive
        if bet <= 0:
            await ctx.send(f"{ctx.author.mention}, bet amount must be a positive number.")
            return

        if bet < minBet:
            await ctx.send(f"{ctx.author.mention}, minimum bet is {minBet:,.2f} $QSE.")
            return

        if bet > maxBet:
            await ctx.send(f"{ctx.author.mention}, the maximum bet amount is {maxBet:,.2f} $QSE.")
            return

        if bet > current_balance:
            # Calculate the missing amount needed to complete the transaction including tax.
            missing_amount = bet - current_balance
            await ctx.send(f"{ctx.author.mention}, you do not have enough $QSE to place the bet. You need {missing_amount:,.2f} more $QSE, including Gas, to place this bet.")
            return

        # Flip the coin
        coin_result = random.choice(['heads', 'tails'])

        tax_percentage = self.calculate_tax_percentage(ctx, "sell_etf")
        tax = (bet * Decimal(tax_percentage))
        total_cost = bet + tax

        # Check for negative balance after tax
        if current_balance - total_cost < 0:
            await ctx.send(f"{ctx.author.mention}, you don't have enough $QSE to cover the bet and Gas.")
            return

        # Deduct bet and tax from user's current balance
        new_balance = current_balance - total_cost
        await update_user_balance(self.conn, user_id, new_balance)

        # Check if the user wins
        win = choice.lower() == coin_result

        # Create an embed to display the result
        embed = discord.Embed(title="Coinflip Result", color=discord.Color.gold())
        embed.add_field(name="Your Choice", value=choice.capitalize(), inline=True)
        embed.add_field(name="Coin Landed On", value=coin_result.capitalize(), inline=True)

        if win:
            win_amount = bet * 4  # Payout for correct choice is 2x
            new_balance += win_amount
            embed.add_field(name="Congratulations!", value=f"You won {win_amount:,.2f} $QSE", inline=False)
            await self.casinoTool(ctx, True)
        else:
            embed.add_field(name="Oops!", value=f"You lost {total_cost:,.2f} $QSE including Gas", inline=False)
            await self.casinoTool(ctx, False)

        embed.add_field(name="New Balance", value=f"{new_balance:,.2f} $QSE", inline=False)

        await ctx.send(embed=embed)
        await log_gambling_transaction(ledger_conn, ctx, "Coinflip", bet, f"{'Win' if win else 'Loss'}", new_balance)
        # Transfer the tax amount to the bot's address P3:03da907038
        await update_user_balance(self.conn, get_user_id(self.P3addrConn, self.bot_address), get_user_balance(self.conn, get_user_id(self.P3addrConn, self.bot_address)) + tax)
        await log_transfer(self, ledger_conn, ctx, "P3 Bot", self.bot_address, get_user_id(self.P3addrConn, self.bot_address), tax)
        elapsed_time = timeit.default_timer() - self.casino_timer_start
        self.casino_avg.append(elapsed_time)

    @commands.command(name='slotmachine', aliases=['slots'], help='Play the slot machine. Bet an amount up to 500,000 $QSE.')
    async def slotmachine(self, ctx, bet: int):
        self.casino_timer_start = timeit.default_timer()
        user_id = ctx.author.id
        current_balance = get_user_balance(self.conn, user_id)
        current_timestamp = datetime.utcnow()
        self.last_gamble = [entry for entry in self.last_gamble if (current_timestamp - entry[1]).total_seconds() <= 5]


        if self.check_last_action(self.last_gamble, user_id, current_timestamp):
            await ctx.send("You can't make another play within 5 seconds of your last action.")
            return
        # Add the current action to the list
        self.last_gamble.append((user_id, current_timestamp))

        # Check if bet amount is positive and within the limit
        if bet <= 0:
            await ctx.send(f"{ctx.author.mention}, bet amount must be a positive number.")
            return

        if bet < minBet:
            await ctx.send(f"{ctx.author.mention}, minimum bet is {minBet:,.2f} $QSE.")
            return

        if bet > maxBet:
            await ctx.send(f"{ctx.author.mention}, the maximum bet amount is {maxBet:,.2f} $QSE.")
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
            payout_multiplier = 25  # 2 in a row with 2.25% payout
        else:
            payout_multiplier = 0

        win_amount = bet * payout_multiplier

        # Calculate tax based on the bet amount
        tax_percentage = self.calculate_tax_percentage(ctx, "sell_etf")
        tax = (bet * Decimal(tax_percentage))

        # Calculate total cost including tax
        total_cost = bet + tax

        # Check for negative balance after tax
        if current_balance - total_cost < 0:
            await ctx.send(f"{ctx.author.mention}, you don't have enough $QSE to cover the bet and Gas.")
            return

        # Deduct the bet and tax from the user's current balance
        new_balance = current_balance - total_cost
        await update_user_balance(self.conn, user_id, new_balance)

        # Create and send the embed with the slot machine result
        embed = discord.Embed(title="Slot Machine", color=discord.Color.gold())
        embed.add_field(name="Result", value=" ".join(result), inline=False)

        if win_amount > 0:
            new_balance += Decimal(win_amount)
            await update_user_balance(self.conn, user_id, new_balance)
            embed.add_field(name="Congratulations!", value=f"You won {win_amount:,} $QSE!", inline=False)
            await log_gambling_transaction(ledger_conn, ctx, "Slots", bet, f"You won {win_amount} $QSE", new_balance)
            await self.casinoTool(ctx, True)
        else:
            embed.add_field(name="Better luck next time!", value=f"You lost {total_cost:,.2f} $QSE including Gas. Your new balance is {new_balance:,.2f} $QSE.", inline=False)
            await log_gambling_transaction(ledger_conn, ctx, "Slots", bet, f"You lost {total_cost:,.2f} $QSE", new_balance)
            await self.casinoTool(ctx, False)

        embed.set_footer(text=f"Your new balance: {new_balance:,.2f} $QSE")
        await ctx.send(embed=embed)
        # Transfer the tax amount to the bot's address P3:03da907038
        await update_user_balance(self.conn, get_user_id(self.P3addrConn, self.bot_address), get_user_balance(self.conn, get_user_id(self.P3addrConn, self.bot_address)) + tax)
        await log_transfer(self, ledger_conn, ctx, "P3 Bot", self.bot_address, get_user_id(self.P3addrConn, self.bot_address), tax)
        elapsed_time = timeit.default_timer() - self.casino_timer_start
        self.casino_avg.append(elapsed_time)




    @commands.command(name='dice_roll', aliases=["dice"], help='Roll a dice. Bet on a number (1-6).')
    async def dice_roll(self, ctx, chosen_number: int, bet_amount: int):
        self.casino_timer_start = timeit.default_timer()
        user_id = ctx.author.id
        current_balance = get_user_balance(self.conn, user_id)

        # Check if bet amount is positive and within the limit
        if bet_amount <= 0:
            await ctx.send(f"{ctx.author.mention}, bet amount must be a positive number.")
            return

        if bet_amount < minBet:
            await ctx.send(f"{ctx.author.mention}, minimum bet is {minBet:,.2f} $QSE.")
            return

        if bet_amount > maxBet:
            await ctx.send(f"{ctx.author.mention}, the maximum bet amount is {maxBet:,.2f} $QSE.")
            return

        # Check if chosen number is valid
        if chosen_number < 1 or chosen_number > 6:
            await ctx.send("Please choose a number between 1 and 6.")
            return

        # Roll the dice
        dice_result = random.randint(1, 6)

        # Calculate the payout
        if dice_result == chosen_number:
            win_amount = bet_amount * 5  # 5x payout for an exact match
        else:
            win_amount = 0

        # Calculate tax based on the bet amount
        tax_percentage = self.calculate_tax_percentage(ctx, "dice_roll")
        tax = Decimal(bet_amount) * Decimal(tax_percentage)

        # Calculate total cost including tax
        total_cost = Decimal(bet_amount) + tax

        # Check for negative balance after tax
        if current_balance - total_cost < 0:
            await ctx.send(f"{ctx.author.mention}, you don't have enough $QSE to cover the bet and Gas.")
            return

        # Deduct the bet and tax from the user's current balance
        new_balance = current_balance - total_cost
        await update_user_balance(self.conn, user_id, new_balance)

        # Create and send the embed with the dice roll result
        embed = discord.Embed(title="Dice Roll 🎲", color=discord.Color.gold())
        embed.add_field(name="Result", value=f"The dice rolled: {dice_result}", inline=False)

        if win_amount > 0:
            new_balance += win_amount
            await update_user_balance(self.conn, user_id, new_balance)
            embed.add_field(name="Congratulations!", value=f"You guessed right! You won {win_amount:,.2f} $QSE!", inline=False)
            await log_gambling_transaction(ledger_conn, ctx, "Dice Roll", bet_amount, f"You won {win_amount} $QSE", new_balance)
            await self.casinoTool(ctx, True)
        else:
            embed.add_field(name="Better luck next time!", value=f"You lost {total_cost:,.2f} $QSE including Gas. Your new balance is {new_balance:,.2f} $QSE.", inline=False)
            await log_gambling_transaction(ledger_conn, ctx, "Dice Roll", bet_amount, f"You lost {total_cost:,.2f} $QSE", new_balance)
            await self.casinoTool(ctx, False)

        embed.set_footer(text=f"Your new balance: {new_balance:,.2f} $QSE")
        await ctx.send(embed=embed)
        # Transfer the tax amount to the bot's address P3:03da907038
        await update_user_balance(self.conn, get_user_id(self.P3addrConn, self.bot_address), get_user_balance(self.conn, get_user_id(self.P3addrConn, self.bot_address)) + tax)
        await log_transfer(self, ledger_conn, ctx, "P3 Bot", self.bot_address, get_user_id(self.P3addrConn, self.bot_address), tax)
        elapsed_time = timeit.default_timer() - self.casino_timer_start
        self.casino_avg.append(elapsed_time)



    @commands.command(name='higher_lower', aliases=['hl'], help='Play the Higher or Lower card game.')
    async def higher_lower(self, ctx, bet_amount: int):
        user_id = ctx.author.id
        current_balance = get_user_balance(self.conn, user_id)

        # Check if bet amount is positive and within the limit
        if bet_amount <= 0:
            await ctx.send(f"{ctx.author.mention}, bet amount must be a positive number.")
            return

        if bet_amount < minBet:
            await ctx.send(f"{ctx.author.mention}, minimum bet is {minBet:,.2f} $QSE.")
            return

        if bet_amount > maxBet:
            await ctx.send(f"{ctx.author.mention}, the maximum bet amount is {maxBet:,.2f} $QSE.")
            return

        # Draw two random cards
        player_card = random.randint(1, 10)
        dealer_card = random.randint(1, 10)

        # Convert card numbers to emojis
        player_card_emoji = f"{player_card}\U000020e3"
        dealer_card_emoji = "❓"

        # Create the main embed with the game start message
        embed = discord.Embed(title="Higher or Lower", color=discord.Color.blue())
        embed.add_field(name="Your Card", value=player_card_emoji, inline=False)
        embed.add_field(name="Dealer's Card", value=dealer_card_emoji, inline=False)
        embed.add_field(name="Instructions", value="React with ⬆️ for Higher or ⬇️ for Lower.", inline=False)
        message = await ctx.send(embed=embed)

        # Add emojis to the message
        await message.add_reaction("⬆️")
        await message.add_reaction("⬇️")

        # Wait for the user's reaction
        reaction, _ = await self.bot.wait_for("reaction_add", timeout=30.0, check=lambda r, u: u == ctx.author and str(r.emoji) in ["⬆️", "⬇️"])

        # Reveal the dealer's card
        embed.set_field_at(1, name="Dealer's Card", value=f"{dealer_card} {dealer_card}\U000020e3", inline=False)
        await message.edit(embed=embed)

        # Determine the winner
        if (reaction.emoji == "⬆️" and player_card < dealer_card) or (reaction.emoji == "⬇️" and player_card > dealer_card):
            result_message = "You guessed wrong! Better luck next time."
            win_amount = 0
            await self.casinoTool(ctx, False)
        else:
            result_message = f"Congratulations! You guessed right! You won {2 * bet_amount:,.2f} $QSE!"
            win_amount = 2 * bet_amount
            await self.casinoTool(ctx, True)

        # Update user's balance
        new_balance = current_balance + win_amount - bet_amount

        # Send the result message
        embed.set_footer(text=f"Your new balance: {new_balance:,.2f} $QSE")
        embed.add_field(name="Result", value=result_message, inline=False)
        await message.edit(embed=embed)

        # Update user's balance
        await update_user_balance(self.conn, user_id, new_balance)




    @commands.command(name='rpg_stats', help='View your RPG stats.')
    async def rpg_stats(self, ctx):
        try:
            user_id = ctx.author.id
            cursor = self.conn.cursor()

            # Get user's RPG stats
            rpg_stats = cursor.execute("SELECT * FROM users_rpg_stats WHERE user_id=?", (user_id,)).fetchone()


            level, experience = get_user_level(self.conn, user_id)
            exp_req = calculate_experience_for_level(level + 1)
            # Calculate the percentage of experience towards the next level
            percentage = (experience / exp_req) * 100

            # Choose emojis or characters for filled and empty portions with different colors
            filled_char = ":large_blue_diamond:"
            empty_char = ":black_large_square: "   # White color for empty portion

            # Create a level bar using colored emojis or characters
            bar_length = 10
            filled_length = int(bar_length * (percentage / 100))
            level_bar = filled_char * filled_length + empty_char * (bar_length - filled_length)
            city = get_current_city(user_id)
            if city not in self.cities:
                set_current_city(user_id, "StellarHub")


            if rpg_stats:
                # Create an embed to display RPG stats
                embed = Embed(title="Your RPG Stats", color=Colour.blue())

                # Map stat names to corresponding emojis
                stat_emojis = {
                    'hp': '❤️',
                    'atk': '⚔️',
                    'def': '🛡️',
                    'eva': '🎯',
                    'luck': '🍀',
                    'chr': '🎭',
                    'spd': '⚡',
                }

                # Combine current and maximum HP
                hp_value = f"{rpg_stats['cur_hp']}/{rpg_stats['max_hp']}"
                embed.add_field(name="Lvl", value=f"{level:,.0f}" if level != 0 else "0", inline=False)
                embed.add_field(name="Experience", value=f"{experience:,.0f}/{exp_req:,.0f}\n{level_bar} {percentage:.2f}%", inline=False)
                embed.add_field(name="------------------------------------------", value="", inline=False)
                embed.add_field(name=f"{stat_emojis['hp']} HP", value=hp_value, inline=True)

                # Add each remaining stat to the embed
                for stat, emoji in stat_emojis.items():
                    if stat != 'hp':
                        stat_value = rpg_stats[stat]
                        embed.add_field(name=f"{emoji} {stat.capitalize()}", value=stat_value, inline=True)
                embed.add_field(name=f"Current City: ", value=city, inline=True)
                await ctx.send(embed=embed)

            else:
                await ctx.send("You haven't started your RPG adventure yet...\n\ncreating rpg stats...")
                create_user_rpg_stats(user_id)
                await self.rpg_stats(ctx)

        except sqlite3.Error as e:
            print(f"Error retrieving RPG stats for user {user_id}: {e}")



    @commands.command(name='heal', help='Heal another user.')
    async def heal(self, ctx, target):
        try:
            # Assuming you have functions to get and update RPG stats
            target_id = get_user_id(self.P3addrConn, target)
            healer_id = ctx.author.id
            # Get user's and target's RPG stats
            target_stats = get_rpg_stats(target_id)

            # Assuming 'cur_hp' and 'max_hp' are RPG stats

            target_cur_hp = target_stats.get('cur_hp', 0)

            if target_cur_hp == 0 and healer_id == target_id:

                # Perform the healing logic (adjust as needed)
                heal_amount = target_stats['max_hp'] - target_cur_hp  # You can customize the amount of healing
                new_target_hp = min(target_cur_hp + heal_amount, target_stats.get('max_hp', 10))

                # Update the target's current HP
                update_rpg_stat(target_id, 'cur_hp', new_target_hp)

                await ctx.send(f"{target} healed for {heal_amount} HP! 🌟")
            else:
                await ctx.send(f"{target} HP must be 0 to heal")

        except Exception as e:
            print(f"Error in heal command: {e}")
            await ctx.send("An unexpected error occurred. Please try again later.")


    @commands.command(name='setdefaultcity')
    async def set_default_city(self, ctx):
        # Connect to your SQLite database
        cursor = self.conn.cursor()

        try:
            # Update all rows in users_rpg_cities to set current_city to 'StellarHub'
            cursor.execute("""
                UPDATE users_rpg_cities
                SET current_city = 'StellarHub'
            """)
            self.conn.commit()

            await ctx.send("All users' current city has been set to StellarHub.")
        except sqlite3.Error as e:
            print(f"An error occurred while updating default cities: {e}")
            await ctx.send("An error occurred while updating default cities.")

        # Close the database connection
        self.conn.close()



    @commands.command(name='update_city_stats', help='Update city statistics')
    async def update_city_stats_command(self, ctx, city, qse, resources, stocks, etps):
        update_city_stats(city, qse, resources, stocks, etps)
        await ctx.send(f"City statistics updated for {city}.")

    @commands.command(name='city_stats', help='Display the number of people and additional statistics in each city')
    async def city_stats(self, ctx):
        try:
            cursor = self.conn.cursor()

            # Fetch the city statistics (population count)
            cursor.execute("SELECT current_city, COUNT(user_id) FROM users_rpg_cities WHERE current_city IS NOT NULL GROUP BY current_city")
            city_pop = cursor.fetchall()

            # Fetch the user count and new city statistics
            cursor.execute("""
                SELECT c.current_city, s.QSE, s.Resources, s.Stocks, s.ETPs
                FROM users_rpg_cities c
                LEFT JOIN user_city_stats s ON c.current_city = s.city
                WHERE c.current_city IS NOT NULL
                GROUP BY c.current_city
            """)
            city_stats = cursor.fetchall()

            if city_stats and city_pop:
                # Create an embed to display city statistics
                embed = discord.Embed(title='City Statistics', color=discord.Colour.blue())

                for city, qse, resources, stocks, etps in city_stats:
                    # Find the corresponding population count for the city
                    count = next((count for c, count in city_pop if c == city), 0)

                    try:
                        qse_integer = int(qse) if qse is not None else 0
                    except ValueError:
                        # Handle the case where qse is not a valid integer
                        qse_integer = 0

                    embed.add_field(
                        name=city,
                        value=f"Number of People: {count:,.0f}\n\nQSE:\n{qse_integer:,.2f}\n\nResources:\n{resources:,.2f}\n\nStocks:\n{stocks:,.2f}\n\nETPs:\n{etps:,.2f}\n\n",
                        inline=False
                    )

                await ctx.send(embed=embed)
            else:
                await ctx.send("No city statistics available.")

        except sqlite3.Error as e:
            print(f"An error occurred while fetching city statistics: {e}")
            await ctx.send("An error occurred while fetching city statistics. Please try again later.")


    @commands.command(name='travel', help='Travel to a different city')
    async def travel(self, ctx, destination_city):
        async with self.db_semaphore:
            async with self.transaction_lock:
                user_id = ctx.author.id
                current_city = get_current_city(user_id)

                if destination_city not in self.cities:
                    await ctx.send(f"{destination_city} does not exist")
                    return

                if current_city:
                    if current_city == destination_city:
                        await ctx.send(f"You are already in {destination_city}.")
                        return
                    else:
                        last_travel_timestamp = get_last_travel_timestamp(self, user_id)
                        cooldown_duration = timedelta(hours=1)

                        if (datetime.now() - last_travel_timestamp) >= cooldown_duration:
                            await ctx.send(f"Traveling to {destination_city}.")
                            set_last_travel_timestamp(self, user_id, datetime.now())
                            set_last_city(user_id, get_current_city(user_id))
                            set_current_city(user_id, destination_city)
                        else:
                            remaining_cooldown = cooldown_duration - (datetime.now() - last_travel_timestamp)
                            await ctx.send(f"You cannot travel again so soon. Wait for the cooldown. Remaining cooldown: {remaining_cooldown}")
                else:
                    await ctx.send("Error: Unable to retrieve the current city. Try again.")


    @commands.command(name="attack")
    async def attack(self, ctx, target):
        async with self.db_semaphore:
            async with self.transaction_lock:
                try:
                    attacker_id = ctx.author.id
                    defender_id = get_user_id(self.P3addrConn, target)

                    if attacker_id == defender_id:
                        embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, attacker_id)}", color=discord.Colour.red())
                        embed.add_field(name=f"Battle Failed", value=f"Cannot attack yourself")
                        await ctx.send(embed=embed)
                        return

                    attacker_city = get_current_city(attacker_id)
                    defender_city = get_current_city(defender_id)


                    if attacker_city == self.safe_city:
                        embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, attacker_id)}", color=discord.Colour.red())
                        embed.add_field(name=f"Current city is non-PvP", value=f"Your Current City: {attacker_city}")
                        await ctx.send(embed=embed)
                        return

                    if attacker_city != defender_city:
                        embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, attacker_id)}", color=discord.Colour.red())
                        embed.add_field(name=f"{target} is not in the same city as you", value=f"Your Current City: {attacker_city}")
                        await ctx.send(embed=embed)
                        return

                    # Get RPG stats for both the attacker and defender
                    attacker_stats = get_rpg_stats(attacker_id)
                    defender_stats = get_rpg_stats(defender_id)

                    if attacker_stats['cur_hp'] == 0:
                        embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, attacker_id)}", color=discord.Colour.red())
                        embed.add_field(name="Your HP is 0", value="")
                        await ctx.send(embed=embed)
                        return

                    if defender_stats['cur_hp'] == 0:
                        embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, attacker_id)}", color=discord.Colour.red())
                        embed.add_field(name=f"{get_p3_address(self.P3addrConn, defender_id)} already at 0 HP", value="")
                        await ctx.send(embed=embed)
                        return

                    if attacker_stats and defender_stats:
                        # Calculate damage based on attacker's stats
                        damage = calculate_damage(attacker_stats, defender_stats)
                        critical = False
                        if random.randint(1, 50) == 1:
                            damage = damage * random.randint(2, 5)
                            critical = True

                        # Apply damage to defender's current HP
                        defender_current_hp = check_current_hp(defender_id)
                        new_defender_hp = max(0, defender_current_hp - damage)

                        experience, level = get_user_experience_info(self.conn, defender_id)
                        if new_defender_hp == 0:
#                            current_balance = float(get_user_balance(self.conn, attacker_id))
#                            winnings = max(current_balance * 0.0001), 1 #.001% of loser QSE with a minimum of 1 QSE
#                            await self.give_addr(ctx, PBotAddr, int(winnings), False)
                            await add_experience(self, self.conn, attacker_id, (random.randint(10, 50) + level) / 2, ctx)
                            return

                        # Update defender's current HP
                        update_current_hp(defender_id, new_defender_hp)

                        # Send DM to the defender
                        defender_member = ctx.guild.get_member(defender_id)
                        if defender_member:
                            defender_dm_channel = await defender_member.create_dm()

                            # Include attack details in the DM
                            attack_details = f"Attacker: {get_p3_address(self.P3addrConn, attacker_id)}\n"
                            attack_details += f"Defender: {get_p3_address(self.P3addrConn, defender_id)}\n"
                            attack_details += f"Damage Dealt: {damage} HP\n"
                            attack_details += f"Current HP: {new_defender_hp}/{defender_stats['max_hp']} HP"

                            await defender_dm_channel.send(attack_details)

                        if damage != 0 and critical:
                            # Create an embed to display the attack result
                            embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, attacker_id)} Fires a shot at {get_p3_address(self.P3addrConn, defender_id)}", color=discord.Colour.yellow())
                            embed.add_field(name="Critical Damage Dealt", value=f"{damage} HP")
                            embed.add_field(name="Defender's Current HP", value=f"{new_defender_hp}/{defender_stats['max_hp']} HP")

                            await add_experience(self, self.conn, attacker_id, (damage * level) / 2, ctx)

                            await ctx.send(embed=embed)

                            if new_defender_hp == 0:
                                embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, attacker_id)} has killed {get_p3_address(self.P3addrConn, defender_id)}", color=discord.Colour.green())
                                await ctx.send(embed=embed)
                                return

                        elif damage != 0 and not critical:
                            # Create an embed to display the attack result
                            embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, attacker_id)} Fires a shot at {get_p3_address(self.P3addrConn, defender_id)}", color=discord.Colour.yellow())
                            embed.add_field(name="Damage Dealt", value=f"{damage} HP")
                            embed.add_field(name="Defender's Current HP", value=f"{new_defender_hp}/{defender_stats['max_hp']} HP")

                            await add_experience(self, self.conn, attacker_id, damage / 2, ctx)

                            await ctx.send(embed=embed)



                            if new_defender_hp == 0:
                                embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, attacker_id)} has killed {get_p3_address(self.P3addrConn, defender_id)}", color=discord.Colour.green())
                                await ctx.send(embed=embed)
                                return

                        else:
                            embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, attacker_id)} Fires a shot at {get_p3_address(self.P3addrConn, defender_id)}", color=discord.Colour.red())
                            embed.add_field(name="Damage Dealt", value=f"{damage} HP")
                            embed.add_field(name="Attack Missed...", value="")

                            await ctx.send(embed=embed)

                        # Simulate defender's attack back once
                        defender_damage = calculate_damage(defender_stats, attacker_stats)
                        defender_critical = False
                        if random.randint(1, 50) == 1:
                            defender_damage = defender_damage * random.randint(2, 5)
                            defender_critical = True

                        # Apply damage to attacker's current HP
                        attacker_current_hp = check_current_hp(attacker_id)
                        new_attacker_hp = max(0, attacker_current_hp - defender_damage)

                        experience, level = get_user_experience_info(self.conn, defender_id)
                        if new_attacker_hp == 0:
                            await add_experience(self, self.conn, defender_id, (random.randint(10, 50) * level) / 2, ctx)

                        # Update defender's current HP
                        update_current_hp(attacker_id, new_attacker_hp)

                        if defender_damage != 0 and defender_critical:
                            # Create an embed to display the defender's counter-attack result
                            embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, defender_id)} Counters with a shot at {get_p3_address(self.P3addrConn, attacker_id)}", color=discord.Colour.yellow())
                            embed.add_field(name="Critical Damage Dealt", value=f"{defender_damage} HP")
                            embed.add_field(name="Attacker's Current HP", value=f"{new_attacker_hp}/{attacker_stats['max_hp']} HP")

                            await add_experience(self, self.conn, defender_id, (defender_damage * level) / 2, ctx)

                            await ctx.send(embed=embed)

                            if new_attacker_hp == 0:
                                embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, defender_id)} has killed {get_p3_address(self.P3addrConn, attacker_id)}", color=discord.Colour.green())
                                await ctx.send(embed=embed)
                                return

                        elif defender_damage != 0 and not defender_critical:
                            # Create an embed to display the defender's counter-attack result
                            embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, defender_id)} Counters with a shot at {get_p3_address(self.P3addrConn, attacker_id)}", color=discord.Colour.yellow())
                            embed.add_field(name="Damage Dealt", value=f"{defender_damage} HP")
                            embed.add_field(name="Attacker's Current HP", value=f"{new_attacker_hp}/{attacker_stats['max_hp']} HP")

                            await add_experience(self, self.conn, defender_id, defender_damage  / 2, ctx)

                            await ctx.send(embed=embed)

                            if new_attacker_hp == 0:
                                embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, defender_id)} has killed {get_p3_address(self.P3addrConn, attacker_id)}", color=discord.Colour.green())
                                await ctx.send(embed=embed)
                                return

                        else:
                            embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, defender_id)} Counters with a shot at {get_p3_address(self.P3addrConn, attacker_id)}", color=discord.Colour.red())
                            embed.add_field(name="Damage Dealt", value=f"{defender_damage} HP")
                            embed.add_field(name="Attack Missed...", value="")

                            await ctx.send(embed=embed)

                    else:
                        await ctx.send("Error retrieving RPG stats for attacker or defender.")

                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
                    await ctx.send("An unexpected error occurred during the attack. Please try again later.")


    @commands.command(name="b2d")
    @is_allowed_user(930513222820331590, PBot)
    async def b2d(self, ctx, target):
        async with self.db_semaphore:
            async with self.transaction_lock:
                try:
                    attacker_id = ctx.author.id
                    defender_id = get_user_id(self.P3addrConn, target)

                    if attacker_id == defender_id:
                        embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, attacker_id)}", color=discord.Colour.red())
                        embed.add_field(name=f"Battle Failed", value=f"Cannot battle yourself")
                        await ctx.send(embed=embed)
                        return

                    attacker_city = get_current_city(attacker_id)
                    defender_city = get_current_city(defender_id)

                    if attacker_city == self.safe_city:
                        embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, attacker_id)}", color=discord.Colour.red())
                        embed.add_field(name=f"Current city is non-PvP", value=f"Your Current City: {attacker_city}")
                        await ctx.send(embed=embed)
                        return

                    if attacker_city != defender_city:
                        embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, attacker_id)}", color=discord.Colour.red())
                        embed.add_field(name=f"{target} is not in the same city as you", value=f"Your Current City: {attacker_city}")
                        await ctx.send(embed=embed)
                        return

                    # Get RPG stats for both the attacker and defender
                    attacker_stats = get_rpg_stats(attacker_id)
                    defender_stats = get_rpg_stats(defender_id)

                    if attacker_stats['cur_hp'] == 0:
                        embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, attacker_id)}", color=discord.Colour.red())
                        embed.add_field(name="Your HP is 0", value="")
                        await ctx.send(embed=embed)
                        return

                    if defender_stats['cur_hp'] == 0:
                        embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, attacker_id)}", color=discord.Colour.red())
                        embed.add_field(name=f"{get_p3_address(self.P3addrConn, defender_id)} already at 0 HP", value="")
                        await ctx.send(embed=embed)
                        return

                    # Continue attacking until one of the players dies
                    while attacker_stats['cur_hp'] > 0 and defender_stats['cur_hp'] > 0:
                        # Calculate damage and apply it to defender's HP
                        damage = calculate_damage(attacker_stats, defender_stats)
                        defender_current_hp = check_current_hp(defender_id)
                        new_defender_hp = max(0, defender_current_hp - damage)
                        update_current_hp(defender_id, new_defender_hp)

                        # Wait for 5 seconds before the next attack
                        await asyncio.sleep(0.5)

                        # Calculate damage from defender's counter-attack and apply it to attacker's HP
                        defender_damage = calculate_damage(defender_stats, attacker_stats)
                        attacker_current_hp = check_current_hp(attacker_id)
                        new_attacker_hp = max(0, attacker_current_hp - defender_damage)
                        update_current_hp(attacker_id, new_attacker_hp)

                        # Wait for 5 seconds before the next attack
                        await asyncio.sleep(0.5)

                        # Create embeds to display each attack
                        attacker_embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, attacker_id)}'s Attack", color=discord.Colour.red())
                        attacker_embed.add_field(name="Damage Dealt", value=f"{damage} HP")
                        attacker_embed.add_field(name="Defender's Current HP", value=f"{new_defender_hp}/{defender_stats['max_hp']} HP")

                        defender_embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, defender_id)}'s Counter-Attack", color=discord.Colour.blue())
                        defender_embed.add_field(name="Damage Dealt", value=f"{defender_damage} HP")
                        defender_embed.add_field(name="Attacker's Current HP", value=f"{new_attacker_hp}/{attacker_stats['max_hp']} HP")

                        # Send embeds for each attack
                        await ctx.send(embed=attacker_embed)
                        await ctx.send(embed=defender_embed)

                    # Check and announce the winner
                    if attacker_stats['cur_hp'] == 0:
                        winner_id = defender_id
                        loser_id = attacker_id
                    else:
                        winner_id = attacker_id
                        loser_id = defender_id

                    winner_embed = discord.Embed(title=f"{get_p3_address(self.P3addrConn, winner_id)} is victorious!", color=discord.Colour.green())
                    winner_embed.add_field(name=f"Winner: {get_p3_address(self.P3addrConn, winner_id)}", value=f"Loser: {get_p3_address(self.P3addrConn, loser_id)}")
                    await ctx.send(embed=winner_embed)

                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
                    await ctx.send("An unexpected error occurred during the battle. Please try again later.")


##




    @commands.command(name='buy_option')
    @is_allowed_user(930513222820331590, PBot)
    async def buy_option(self, ctx, asset, amount=1):
        updown_assets = ["BlueChipOG"]
        updown_assets.extend(self.etfs)

        if asset not in updown_assets:
            await ctx.send(f"UpDown Options not available for {asset}")
            return

        max_contracts_limit = 100_000_000_000_000

        user_id = ctx.author.id

        # Check if the user has reached the contract limit for the specified asset
        current_contracts_count = count_user_contracts(self, user_id, asset)
        if current_contracts_count + amount > max_contracts_limit:
            await ctx.send(f"You have reached the maximum limit of {max_contracts_limit} contracts for asset {asset}.")
            return


        P3addrConn = sqlite3.connect("P3addr.db")
        PBotAddr = get_p3_address(P3addrConn, PBot)

        if asset in self.etfs:
            current_price = await get_etf_value(self.conn, int(asset))
            exp_day = 1
        else:
            current_price = await get_stock_price(self, ctx, asset)
            exp_day = 1
            self.UpDownPer = self.UpDownPer * 2

        lower_limit, upper_limit = calculate_option_range(self, current_price)
        contract_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        expiration = (datetime.now() + timedelta(days=exp_day)).strftime("%Y-%m-%d %H:%M:%S")
        current_balance = get_user_balance(self.conn, user_id)

        if current_price * amount > current_balance:
            await ctx.send(f"Current Balance: {current_balance:,.2f} is less than order amount {current_price * amount:,.2f}")
            return
        await self.give_addr(ctx, PBotAddr, int(current_price * amount), False)

        orders = [{
            'user_id': user_id,
            'asset': asset,
            'current_price': current_price,
            'lower_limit': lower_limit,
            'upper_limit': upper_limit,
            'contract_date': contract_date,
            'expiration': expiration
        } for _ in range(amount)]

        # Call the new function to add multiple UpDown orders in a single transaction
        success = await add_multiple_updown_orders(self, orders)

        if success:
            potential_payout = (upper_limit - lower_limit) * self.UpDownPer / 100

            if asset in self.etfs:
                tail = f"ETF {asset}"
            else:
                tail = asset

            embed = discord.Embed(
                title=f"UpDown Option Contract purchase for {tail}",
                color=discord.Color.green()
            )
            embed.add_field(name="Contracts Purchased:", value=f"{amount:,.0f}\n\n", inline=False)
            embed.add_field(name="Current Price:", value=f"{current_price:,.2f}", inline=False)
            embed.add_field(name="Lower Limit:", value=f"{lower_limit:,.2f}", inline=False)
            embed.add_field(name="Upper Limit:", value=f"{upper_limit:,.2f}", inline=False)
            embed.add_field(name="Contract:", value=f"{contract_date}", inline=False)
            embed.add_field(name="Expiration:", value=f"{expiration}", inline=False)
            embed.add_field(name="Potential Payout:", value=f"Contract Price: {current_price:,.2f}\n\nOrder Price: {(current_price * amount):,.2f}\n\nPayout Price: {potential_payout:,.2f}\n\nTotal Winnings: {((current_price * amount) + (potential_payout * amount)):,.2f}", inline=False)

            await ctx.send(embed=embed)
        else:
            await ctx.send("Failed to add UpDown option contracts. Please try again.")




    @commands.command(name='show_options')
    async def show_options(self, ctx):
        user_id = ctx.author.id
        expiration_status = check_expiration_status(self, user_id)

        if expiration_status:
            await ctx.send("You don't have any active option contracts.")
            return

        user_options = await get_user_options(self, user_id)

        if user_options:
            asset_data = {}  # Dictionary to store compiled data for each asset

            for index, user_option in enumerate(user_options, start=1):
                buy_price = user_option["current_price"]
                asset = user_option["asset"]
                if asset in self.etfs:
                    current_price = await get_etf_value(self.conn, asset)
                else:
                    current_price = await get_stock_price(self, ctx, asset)
#                    self.UpDownPer = self.UpDownPer * 2

                # Calculate the potential payout based on the percentage range and payout percentage
                lower_limit = user_option["lower_limit"]
                upper_limit = user_option["upper_limit"]
                potential_payout = (upper_limit - lower_limit) * self.UpDownPer / 100
                payout = buy_price + potential_payout

                # Compile data for each asset
                if asset not in asset_data:
                    asset_data[asset] = {
                        'total_contracts': 1,
                        'total_value': buy_price,
                        'total_reward': payout
                    }
                else:
                    asset_data[asset]['total_contracts'] += 1
                    asset_data[asset]['total_value'] += buy_price
                    asset_data[asset]['total_reward'] += payout

            # Send compiled data for each asset
            for asset, data in asset_data.items():
                embed = discord.Embed(
                    title=f"UpDown Option Contract Details - {asset}",
                    color=discord.Color.green()
                )
                embed.add_field(name="Total Contracts", value=str(data['total_contracts']), inline=False)
                embed.add_field(name="Total Value", value=f"{data['total_value']:,.2f} $QSE", inline=False)
                embed.add_field(name="Total Reward", value=f"{data['total_reward']:,.2f} $QSE", inline=False)

                await ctx.send(embed=embed)

        else:
            await ctx.send("You don't have any active option contracts.")



    @commands.command(name='updown')
    async def updown(self, ctx, asset):
        if asset in self.etfs:
            current_price = await get_etf_value(self.conn, int(asset))
        else:
            current_price = await get_stock_price(self, ctx, asset)
#            self.UpDownPer = self.UpDownPer * 2

        lower_limit, upper_limit = calculate_option_range(self, current_price)


        # Calculate the potential payout based on the percentage range and payout percentage
        potential_payout = (upper_limit - lower_limit) * self.UpDownPer / 100

        embed = discord.Embed(
            title=f"UpDown Option Contract",
            color=discord.Color.blue()
        )
        embed.add_field(name="Current Price:", value=f"{current_price:,.2f}", inline=False)
        embed.add_field(name="Lower Limit:", value=f"{lower_limit:,.2f}", inline=False)
        embed.add_field(name="Upper Limit:", value=f"{upper_limit:,.2f}", inline=False)
        embed.add_field(name="Potential Payout:", value=f"Contract Price: {current_price:,.2f}\n\nPayout Price: {potential_payout:,.2f}\n\nTotal Winnings: {(current_price + potential_payout):,.2f}", inline=False)

        await ctx.send(embed=embed)



    @commands.command(name='stats', aliases=['portfolio'], help='Displays the user\'s financial stats.')
    async def stats(self, ctx, address: str = None):
        self.tax_command_timer_start = timeit.default_timer()
        async with self.db_semaphore:
            async with self.transaction_lock:
                mv = await get_etf_value(self.conn, 6)
                await add_mv_metric(self, ctx, mv)
                await add_reserve_metric(self, ctx)
                if address is None:
                    user_id = ctx.author.id
                    P3Addr = generate_crypto_address(user_id)
                else:
                    P3Addr = address
                    user_id = get_user_id(self.P3addrConn, address)

                try:
                    # Connect to databases using context managers
                    with sqlite3.connect('currency_system.db') as conn, sqlite3.connect("p3ledger.db") as ledger_conn:
                        cursor = conn.cursor()
                        ledger_cursor = ledger_conn.cursor()

                        try:
                            # Get user balance
                            current_balance = cursor.execute("SELECT COALESCE(balance, 0) FROM users WHERE user_id=?", (user_id,)).fetchone()[0]
                        except Exception as e:
                            current_balance = 0

                        try:
                            # Calculate total stock value
                            user_stocks = cursor.execute("SELECT symbol, amount FROM user_stocks WHERE user_id=?", (user_id,)).fetchall()
                            total_stock_value = sum(price * amount for symbol, amount in user_stocks
                                                    for price in cursor.execute("SELECT COALESCE(price, 0) FROM stocks WHERE symbol=?", (symbol,)).fetchone())
                        except Exception as e:
                            total_stock_value = 0

                        try:
                            # Calculate total ETF value
                            user_etfs = cursor.execute("SELECT etf_id, quantity FROM user_etfs WHERE user_id=?", (user_id,)).fetchall()
                            total_etf_value = sum(etf_value * quantity for etf_id, quantity in user_etfs
                                                  for etf_value in cursor.execute("""
                                                      SELECT COALESCE(SUM(stocks.price * etf_stocks.quantity), 0)
                                                      FROM etf_stocks JOIN stocks ON etf_stocks.symbol = stocks.symbol
                                                      WHERE etf_stocks.etf_id=? GROUP BY etf_stocks.etf_id""", (etf_id,)).fetchone())
                        except Exception as e:
                            total_etf_value = 0

                        try:
                            # Calculate total buys, sells, and profits/losses from p3ledger for stocks within the current month
                            current_month_start = datetime.datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                            stock_transactions_data = ledger_cursor.execute("""
                                SELECT action, SUM(quantity), SUM(price)
                                FROM stock_transactions
                                WHERE user_id=? AND (action='Buy Stock' OR action='Sell Stock') AND timestamp >= ?
                                GROUP BY action
                            """, (user_id, current_month_start)).fetchall()

                            # Adding default values for cases where data is not available
                            total_stock_buys = sum(total_price for action, quantity, total_price in stock_transactions_data if "Buy" in action) or 0
                            total_stock_sells = sum(total_price for action, quantity, total_price in stock_transactions_data if "Sell" in action) or 0
                            total_stock_profits_losses = total_stock_sells - total_stock_buys
                        except Exception as e:
                            total_stock_buys = total_stock_sells = total_stock_profits_losses = 0

                        try:
                            # Calculate total buys, sells, and profits/losses from p3ledger for ETFs within the current month
                            etf_transactions_data = ledger_cursor.execute("""
                                SELECT action, SUM(quantity), SUM(price)
                                FROM stock_transactions
                                WHERE user_id=? AND (action='Buy ETF' OR action='Sell ETF') AND timestamp >= ?
                                GROUP BY action
                            """, (user_id, current_month_start)).fetchall()

                            # Adding default values for cases where data is not available
                            total_etf_buys = sum(total_price for action, quantity, total_price in etf_transactions_data if "Buy" in action) or 0
                            total_etf_sells = sum(total_price for action, quantity, total_price in etf_transactions_data if "Sell" in action) or 0
                            total_etf_profits_losses = total_etf_sells - total_etf_buys
                        except Exception as e:
                            total_etf_buys = total_etf_sells = total_etf_profits_losses = 0

                        try:
                            # Calculate the total value of metals in the user's inventory
                            metal_values = cursor.execute("""
                                SELECT items.price * inventory.quantity
                                FROM inventory
                                JOIN items ON inventory.item_id = items.item_id
                                WHERE user_id=? AND items.item_name IN ('Copper', 'Platinum', 'Gold', 'Silver', 'Lithium')
                            """, (user_id,)).fetchall()

                            # Adding default value for cases where data is not available
                            total_metal_value = sum(metal_value[0] for metal_value in metal_values) or 0
                        except Exception as e:
                            total_metal_value = 0

                        # Get P3:Stable value
                        stable_stock = "P3:Stable"
                        stable_stock_price = await get_stock_price(self, ctx, stable_stock) or 0
                        user_owned_stable = self.get_user_stock_amount(user_id, stable_stock) or 0
                        user_stable_value = stable_stock_price * user_owned_stable

                        # Calculate total value of all funds
                        total_funds_value = current_balance + total_stock_value + total_etf_value + total_metal_value

                        current_month_name = calendar.month_name[datetime.now().month]


                        # Create the embed
                        embed = Embed(title=f"{P3Addr} Financial Stats", color=Colour.green())
                        embed.set_thumbnail(url="attachment://market-stats.jpeg")
                        embed.add_field(name="Balance", value=f"{current_balance:,.0f} $QSE" if current_balance != 0 else "0", inline=False)
                        embed.add_field(name="Total Stock Value", value=f"{total_stock_value:,.0f} $QSE" if total_stock_value != 0 else "0", inline=False)
                        embed.add_field(name="Total ETP Value", value=f"{total_etf_value:,.0f} $QSE" if total_etf_value != 0 else "0", inline=False)
                        embed.add_field(name="P3:Stable Value", value=f"{user_stable_value:,.0f} $QSE" if user_stable_value != 0 else "0", inline=False)
                        embed.add_field(name="Total Metal Value", value=f"{total_metal_value:,.2f} $QSE" if total_metal_value != 0 else "0", inline=False)
                        shares_in_escrow = await get_total_shares_in_escrow_user(self, user_id)
                        embed.add_field(name="Total Escrow", value=f"{shares_in_escrow:,.0f} shares" if shares_in_escrow != 0 else "0", inline=False)
                        embed.add_field(name="Total Funds Value", value=f"{total_funds_value:,.0f} $QSE" if total_funds_value != 0 else "0", inline=False)

                        await ctx.send(embed=embed, file=File("./stock_images/market-stats.jpeg"))
                        await tax_command(self, ctx)
                        elapsed_time = timeit.default_timer() - self.tax_command_timer_start
                        self.tax_command_avg.append(elapsed_time)
                        avg_time = sum(self.tax_command_avg) / len(self.tax_command_avg)

                        await self.rpg_stats(ctx)

                except sqlite3.Error as e:
                    print(f"Database error: {e}")
                    await ctx.send("An error occurred while retrieving your stats. Please try again later.")

                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
                    await ctx.send("An unexpected error occurred. Please try again later.")



    @commands.command(name="reserve_funds")
    async def reserve_funds(self, ctx):
        Pbot_Balance = 0
        cursor = self.conn.cursor()
        try:
            # Calculate total stock value
            cursor.execute("SELECT balance FROM users WHERE user_id = ?", (PBot,))
            row = cursor.fetchone()
            Pbot_Balance = row[0]
            # Calculate total stock value
            cursor.execute("SELECT symbol, amount FROM user_stocks WHERE user_id=?", (PBot,))
            user_stocks = cursor.fetchall()
            total_stock_value = 0

            for symbol, amount in user_stocks:
                cursor.execute("SELECT price FROM stocks WHERE symbol=?", (symbol,))
                stock_price_row = cursor.fetchone()
                stock_price = stock_price_row[0] if stock_price_row else 0
                total_stock_value += stock_price * amount
        except Exception as e:
            total_stock_value = 0

        MAX_BALANCE = Pbot_Balance + total_stock_value

        embed = discord.Embed(
            title=f"Reserve Balance Sheet",
            color=discord.Color.blue()
        )
        embed.add_field(name="Reserve Funds:", value=f"{Pbot_Balance:,.2f}", inline=False)
        embed.add_field(name="Reserve Stocks:", value=f"{total_stock_value:,.2f}", inline=False)
        embed.add_field(name="Reserve Balance:", value=f"{MAX_BALANCE:,.2f}", inline=False)

        await ctx.send(embed=embed)
        await self.reserve_chart(ctx)


    @commands.command(name='total_stats', aliases=['total_portfolio', 'market_stats'], help='Displays the total financial stats of all users.')
    @is_allowed_user(930513222820331590, PBot)
    async def total_stats(self, ctx):
        self.is_halted = True
        self.is_halted_order = True
        try:
            Pbot_Balance = 0
            # Connect to databases using context managers
            with sqlite3.connect('currency_system.db') as conn, sqlite3.connect("p3ledger.db") as ledger_conn, sqlite3.connect("P3addr.db") as addr_conn:
                cursor = conn.cursor()
                ledger_cursor = ledger_conn.cursor()
                addr_cursor = addr_conn.cursor()

                # Calculate Total Escrow Value
                cursor.execute("SELECT symbol, available, total_supply, price FROM stocks")
                stocks = cursor.fetchall()
                total_escrow_value = 0
                for symbol in stocks:
                    escrow_value = 0
                    total_shares_in_orders = await get_total_shares_in_orders(self, symbol[0])
                    if total_shares_in_orders:
                        avg_buy, avg_sell = await calculate_average_prices_by_symbol(self, symbol[0])
                        avg_price = (avg_buy + avg_sell) / 2
                        escrow_value = total_shares_in_orders * avg_price
                        total_escrow_value += escrow_value

                # Calculate total stock value
                cursor.execute("SELECT balance FROM users WHERE user_id = ?", (PBot,))
                row = cursor.fetchone()
                Pbot_Balance = row[0]

                # Calculate total stock value
                cursor.execute("SELECT symbol, amount FROM user_stocks WHERE user_id=?", (PBot,))
                user_stocks = cursor.fetchall()
                reserve_stock_value = 0

                for symbol, amount in user_stocks:
                    cursor.execute("SELECT price FROM stocks WHERE symbol=?", (symbol,))
                    stock_price_row = cursor.fetchone()
                    stock_price = stock_price_row[0] if stock_price_row else 0
                    reserve_stock_value += stock_price * amount

                reserve_stock_value = reserve_stock_value - total_escrow_value
                MAX_BALANCE = Pbot_Balance + reserve_stock_value

                # Get all user IDs from P3addr.db
                user_ids_result = addr_cursor.execute("SELECT DISTINCT user_id FROM user_addresses").fetchall()
                all_user_ids = [user_id for user_id, in user_ids_result]

                # Initialize total values
                total_balance = 0
                total_stock_value = 0
                total_etf_value = 0
                total_stable_value = 0
                total_metal_value = 0
                total_funds_value = 0
                user_qse = 0
                total_updown_value = get_total_current_prices(self)

                # Iterate over each user
                for user_id in all_user_ids:
                    try:
                        # Get user balance
                        current_balance = float(get_user_balance(conn, user_id))
                        user_qse += current_balance

                        # Calculate total stock value
                        user_stocks = cursor.execute("SELECT symbol, amount FROM user_stocks WHERE user_id=?", (user_id,)).fetchall()
                        total_stock_value += sum(price * amount for symbol, amount in user_stocks
                                                  for price in cursor.execute("SELECT COALESCE(price, 0) FROM stocks WHERE symbol=?", (symbol,)).fetchone())

                        # Calculate total ETF value
                        user_etfs = cursor.execute("SELECT etf_id, quantity FROM user_etfs WHERE user_id=?", (user_id,)).fetchall()
                        total_etf_value += sum(etf_value * quantity for etf_id, quantity in user_etfs
                                               for etf_value in cursor.execute("""
                                                   SELECT COALESCE(SUM(stocks.price * etf_stocks.quantity), 0)
                                                   FROM etf_stocks JOIN stocks ON etf_stocks.symbol = stocks.symbol
                                                   WHERE etf_stocks.etf_id=? GROUP BY etf_stocks.etf_id""", (etf_id,)).fetchone())

                        # Calculate total value of metals in the user's inventory
                        metal_values = cursor.execute("""
                            SELECT items.price * inventory.quantity
                            FROM inventory
                            JOIN items ON inventory.item_id = items.item_id
                            WHERE user_id=? AND items.item_name IN ('Copper', 'Platinum', 'Gold', 'Silver', 'Lithium')
                        """, (user_id,)).fetchall()
                        total_metal_value += sum(metal_value[0] for metal_value in metal_values) or 0

                        cursor.execute("SELECT symbol, available, price, total_supply FROM stocks")
                        stocks_data = cursor.fetchall()

                        # Calculate the total value of all stocks
                        market_stock_value = sum(price * total_supply for symbol, available, price, total_supply in stocks_data)

                        # Get P3:Stable value
                        stable_stock = "P3:Stable"
                        stable_stock_price = await get_stock_price(self, ctx, stable_stock) or 0
                        user_owned_stable = self.get_user_stock_amount(user_id, stable_stock) or 0
                        total_stable_value += stable_stock_price * user_owned_stable
                        bank_qse = await get_total_qse_deposited(self)
                        Pbot_Balance = Pbot_Balance - bank_qse

                        # Calculate total funds value for each user
                        user_balance = user_qse + total_stock_value + total_etf_value + total_escrow_value + total_updown_value + total_metal_value - Pbot_Balance - reserve_stock_value
                        total_balance = user_qse + total_stock_value + total_etf_value + total_metal_value + total_escrow_value + total_updown_value

                    except Exception as e:
                        print(f"An error occurred for user {user_id}: {e}")

                # Create the embed for total stats
                embed = Embed(title="Market Financial Book", color=Colour.green())
                embed.add_field(name="Community QSE:", value=f"{(user_qse - Pbot_Balance):,.0f} $QSE\n({(((user_qse - Pbot_Balance) / total_balance) * 100):,.4f}%)", inline=False)
                embed.add_field(name="Community Stock Value:", value=f"{(total_stock_value - reserve_stock_value):,.0f} $QSE\n(Market: {(((total_stock_value - reserve_stock_value) / total_balance) * 100):,.4f}%)\n(Total Stock Supply: {(((total_stock_value - reserve_stock_value) / market_stock_value) * 100):,.4f}%)", inline=False)
                embed.add_field(name="Community ETP Value:", value=f"{total_etf_value:,.0f} $QSE\n({((total_etf_value / total_balance) * 100):,.4f}%)", inline=False)
                embed.add_field(name="Community P3:Stable Value:", value=f"{total_stable_value:,.0f} $QSE\n({((total_stable_value / total_balance) * 100):,.4f}%)", inline=False)
                embed.add_field(name="Community Metal Value:", value=f"{total_metal_value:,.0f} $QSE\n({((total_metal_value / total_balance) * 100):,.4f}%)", inline=False)
                embed.add_field(name="Community Escrow Value:", value=f"{total_escrow_value:,.0f} $QSE\n({((total_escrow_value / total_balance) * 100):,.4f}%)", inline=False)
                embed.add_field(name="Community UpDown Value:", value=f"{total_updown_value:,.0f} $QSE\n({((total_updown_value / total_balance) * 100):,.4f}%)", inline=False)
                embed.add_field(name="Community Asset Value:", value=f"{user_balance:,.0f} $QSE\n({((user_balance / total_balance) * 100):,.4f}%)", inline=False)
                embed.add_field(name="", value="---------------------------------------------", inline=False)
                embed.add_field(name="Reserve QSE:", value=f"{Pbot_Balance:,.0f} $QSE\n({((Pbot_Balance / total_balance) * 100):,.4f}%)", inline=False)
                embed.add_field(name="Reserve Bank QSE:", value=f"{bank_qse:,.0f} $QSE\n({((bank_qse / total_balance) * 100):,.4f}%)", inline=False)
                embed.add_field(name="Reserve Stock Value:", value=f"{reserve_stock_value:,.0f} $QSE\nMarket: ({((reserve_stock_value / total_balance) * 100):,.4f}%)\nTotal Stock Supply: ({((reserve_stock_value / market_stock_value) * 100):,.4f}%)", inline=False)
                embed.add_field(name="Reserve Funds:", value=f"{MAX_BALANCE:,.0f} $QSE\n({((MAX_BALANCE / total_balance) * 100):,.4f}%)", inline=False)
                embed.add_field(name="", value="---------------------------------------------", inline=False)
                embed.add_field(name="Total Stock Supply Value:", value=f"{market_stock_value:,.0f} $QSE", inline=False)
                embed.add_field(name="Total Market Value:", value=f"{total_balance:,.0f} $QSE", inline=False)

                await ctx.send(embed=embed)
                await  self.market_chart(ctx)
                await self.reserve_chart(ctx)
                self.is_halted = False
                self.is_halted_order = False

        except sqlite3.Error as e:
            print(f"Database error: {e}")
            await ctx.send("An error occurred while retrieving total stats. Please try again later.")
            self.is_halted = False
            self.is_halted_order = False

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            await ctx.send("An unexpected error occurred. Please try again later.")
            self.is_halted = False
            self.is_halted_order = False


    @commands.command(name='leaderboard', aliases=['top10'], help='Displays the top 10 P/L for Stocks and ETFs.')
    async def leaderboard(self, ctx):
        try:
            with sqlite3.connect('currency_system.db') as conn, sqlite3.connect("p3ledger.db") as ledger_conn:
                cursor = conn.cursor()
                ledger_cursor = ledger_conn.cursor()

                # Get the top 10 Stocks based on P/L
                top_stock_data = ledger_cursor.execute("""
                    SELECT symbol, SUM(CASE WHEN action='Sell Stock' THEN -price ELSE price END) AS total_pl
                    FROM stock_transactions
                    WHERE action IN ('Buy Stock', 'Sell Stock') AND quantity > 0
                    GROUP BY symbol
                    ORDER BY total_pl DESC
                    LIMIT 10
                """).fetchall()

                # Get the top 10 ETFs based on P/L
                top_etf_data = ledger_cursor.execute("""
                    SELECT etf_id, SUM(CASE WHEN action IN ('Sell ETF', 'Sell ALL ETF') THEN -price ELSE price END) AS total_pl
                    FROM stock_transactions
                    WHERE action IN ('Buy ETF', 'Sell ETF', 'Buy ALL ETF', 'Sell ALL ETF') AND quantity > 0
                    GROUP BY etf_id
                    ORDER BY total_pl DESC
                    LIMIT 10
                """).fetchall()

                # Create the embed for Stocks leaderboard
                stock_leaderboard_embed = Embed(title="Top 10 Stocks P/L", color=Colour.blue())
                for rank, (symbol, total_pl) in enumerate(top_stock_data, start=1):
                    stock_leaderboard_embed.add_field(name=f"{rank}. {symbol}", value=f"Total P/L: {total_pl:,.0f} $QSE", inline=False)

                # Create the embed for ETFs leaderboard
                etf_leaderboard_embed = Embed(title="Top 10 ETFs P/L", color=Colour.orange())
                for rank, (etf_id, total_pl) in enumerate(top_etf_data, start=1):
                    etf_leaderboard_embed.add_field(name=f"{rank}. {etf_id}", value=f"Total P/L: {total_pl:,.0f} $QSE", inline=False)

                await ctx.send(embed=stock_leaderboard_embed)
                await ctx.send(embed=etf_leaderboard_embed)

        except sqlite3.Error as e:
            print(f"Database error: {e}")
            await ctx.send("An error occurred while retrieving the leaderboard. Please try again later.")

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            await ctx.send("An unexpected error occurred. Please try again later.")


    @commands.command(name='check_stocks', help='Check which stocks you can still buy.')
    async def check_stocks(self, ctx):
        user_id = ctx.author.id
        member = ctx.guild.get_member(user_id)

        # Calculate stock limit based on user's role
        dStockLimit = 150000000 * 1.25 if has_role(member, bronze_pass) else 150000000

        # Check if the user has a last checked time stored in the database
        last_checked_time = self.get_last_checked_time(user_id)

        # Get the current UTC time
        current_utc_time = datetime.datetime.utcnow().replace(tzinfo=timezone.utc)

        # Calculate the time elapsed since the last check in hours
        time_elapsed_hours = (current_utc_time - last_checked_time).total_seconds() / 3600 if last_checked_time else 1

        if time_elapsed_hours >= 1:
            # Connect to the database using a context manager
            with sqlite3.connect("currency_system.db") as conn:
                cursor = conn.cursor()

                # Fetch all stock symbols and their available amounts
                cursor.execute("SELECT symbol, available FROM stocks")
                all_stocks = cursor.fetchall()

                # Fetch daily bought amounts for each stock
                cursor.execute("""
                    SELECT symbol, COALESCE(SUM(amount), 0)
                    FROM user_daily_buys
                    WHERE user_id=? AND DATE(timestamp)=DATE('now')
                    GROUP BY symbol
                """, (user_id,))
                hourly_bought_records = dict(cursor.fetchall())

            # Calculate remaining amounts for each stock the user can buy today
            stocks_can_buy = {stock: dStockLimit - hourly_bought_records.get(stock, 0) for stock, _ in all_stocks}

            if stocks_can_buy:
                # Split the stocks into pages with a maximum of 5 stocks per page
                stock_pages = list(chunks(stocks_can_buy, 5))

                # Send the first page
                page_num = 0
                message = await ctx.send(embed=create_stock_page(stock_pages[page_num]), delete_after=60.0)

                # Update the last checked time in the database
                self.update_last_checked_time(user_id, current_utc_time)

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

                    except asyncio.TimeoutError:
                        break
        else:
            remaining_time_minutes = round((1 - time_elapsed_hours) * 60)
            message = f"{ctx.author.mention}, you can check stocks again in {remaining_time_minutes} minutes."
            await ctx.send(message)





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





            # Reward the user
            if random.choice([True, False]):  # 50% chance of not getting any reward
                tokens_reward = random.randint(100, 100000000)
                stocks_reward_symbol = random.choice(available_stocks)
                stocks_reward_amount = random.randint(1, 10000000)


                cursor.execute("SELECT amount FROM user_stocks WHERE user_id=? AND symbol=?", (PBot, stocks_reward_symbol))
                reserve_stock = cursor.fetchone()
                reserve_owned = int(reserve_stock[0]) if reserve_stock else 0

#                if stocks_reward_amount > available_stocks:
#                    await ctx.send("No stocks are available for rewards at the moment. Try again later.")
#                    return

                if stocks_reward_amount > reserve_owned:
                    await ctx.send("No stocks are available for rewards at the moment. Try again later.")
                    return

                target = get_p3_address(self.P3addrConn, user_id)
                await self.send_from_reserve(ctx, user_id, tokens_reward)
                sender_addr = get_p3_address(self.P3addrConn, PBot)
                await send_stock(self, ctx, target, sender_addr, stocks_reward_symbol, stocks_reward_amount)
#                await self.give_stock(ctx, target, stocks_reward_symbol, stocks_reward_amount, False)
                await add_experience(self, self.conn, ctx.author.id, 10, ctx)
                message = f"🛠️ You've worked hard and earned {tokens_reward:,.0f} $QSE and {stocks_reward_amount:,.0f} shares of {stocks_reward_symbol}! and 10xp 🚀"
            else:
                message = "😴 You worked, but it seems luck wasn't on your side this time. Try again later!"

            # Set the cooldown for the user
            self.last_job_times[user_id] = datetime.utcnow()

            await ctx.send(message)
        else:
            await ctx.send(f'Must have at least the Bronze Pass to use this command, check out the Discord Store Page to Purchase')


    @commands.command(name="gun_range")
    async def gun_range(self, ctx):

        if random.choice([True, False]):
            await ctx.send("You trained at the Gun Range and got 50 XP")
            await add_experience(self, self.conn, ctx.author.id, 50, ctx)


        else:
            await ctx.send("Failed to hit your mark at the Gun Range...sorry")
        self.last_range_times[ctx.author.id] = datetime.utcnow()

# Banking







##

async def setup(bot):
    await bot.add_cog(CurrencySystem(bot))
