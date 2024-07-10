import aiohttp
import asyncio
import logging
from cachetools import TTLCache
total_delay_time = 0
# Configure logging
logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)


sharpbookkeys = {"player_assists": ["draftkings"], "player_threes": ["fliff"]}

class Market:
    def __init__(self, id: str, name: str, min_bookmakers: int):
        """
        Initialize a new 'Market' object.
        
        Args:
            id (str): The unique identifier of the market.
            name (str): The descriptive name of the market.
            min_bookmakers (int): The minimum number of bookmakers required for the market.
        """
        self.id = id
        self.name = name
        self.bookmakers = {}  # This will now store bookmaker: {outcome: data} //redo all as sets()? no dicts
        self.sharpbookkey = sharpbookkeys.get(name, ['draftkings'])[0]  # update with flag marked from main()
        self.MIN_BOOKMAKERS=min_bookmakers
        self.compared_games_count = 0
    
    

    def has_enough_data(self) -> bool:
        """
        Determines if the Market object has sufficient data.
        
        This method checks if the Market object has data from a minimum number of bookmakers. 
        The minimum number is defined by the MIN_BOOKMAKERS attribute.
        
        Returns:
            bool: True if the Market object has data from at least MIN_BOOKMAKERS bookmakers, False otherwise.
        """
        return len(self.bookmakers) >= self.MIN_BOOKMAKERS

    def calculate_and_emit_outcomes(self):
        
        # Placeholder for a method to calculate and emit outcomes
        logging.debug("calculate_and_emit_outcomes method called.")
        
    def validate_data(self, game_data):
        """
        Validate the game data.
        
        This method compares the bookmakers and events in the game data with those in the Market object.
        
        Args:
            game_data (dict): The game data to validate. It's a dictionary with keys 'bookmakers' and 'events'.
                                'bookmakers' is a list of dictionaries with key 'key'.
                                'events' is a list of dictionaries with key 'id'.
            
        Returns:
            bool: True if the data is valid, False otherwise.
        """
        logging.debug("Starting data validation.")
        # Extract the bookmakers and events from the game data
        game_bookmakers = [bookmaker['key'] for bookmaker in game_data.get('bookmakers', [])]
        game_events = [event['id'] for event in game_data.get('events', [])]

        # Extract the bookmakers from the Market object
        market_bookmakers = list(self.bookmakers.keys())

        # Log the extracted bookmakers and events for debugging
        logging.debug(f"Game bookmakers: {game_bookmakers}")
        logging.debug(f"Market bookmakers: {market_bookmakers}")
        logging.debug(f"Game data: {game_data}")
        logging.debug(f"Market object bookmakers data: {self.bookmakers}")

        # Compare if the bookmakers match
        if set(game_bookmakers) != set(market_bookmakers):
            logging.warning(f"Bookmakers in Market object do not match game data for market {self.name}.")
            logging.warning(f"Difference: {set(game_bookmakers).symmetric_difference(set(market_bookmakers))}")
            logging.warning(f"Similarity: {set(game_bookmakers).intersection(set(market_bookmakers))}")
            return False

        # Compare if the events match
        for bookmaker, events in self.bookmakers.items():
            for event in events:
                if event not in game_events:
                    logging.warning(f"Event {event} in Market object do not match game data for market {self.name}.")
                    return False

        logging.debug("Data validation completed successfully.")
        return True
    
    def update_market_data(self, market_data, eventid, bookmaker): 
        """
        Update the market data for each bookmaker.
        
        Args:
            market_data (dict): The market data to update. It's a dictionary with keys 'key', 'last_update', and 'outcomes'.
                                'outcomes' is a list of dictionaries with keys 'name', 'description', 'price', 'point'.
            eventid (str): The id of the event.
            bookmaker (str): The name of the bookmaker.
        """
        logging.info(f"Starting to update market data for bookmaker {bookmaker} and event {eventid} with market data: {market_data}")
        # Update the market data for each bookmaker
        decereal_data = self.decereal(market_data, eventid, bookmaker)
        logging.info(f'Decereal data: {decereal_data}')
        if decereal_data is not None and decereal_data['outcomes'] is not None:
            for outcome in decereal_data['outcomes']:
                if bookmaker not in self.bookmakers:
                    self.bookmakers[bookmaker] = {}
                if eventid not in self.bookmakers[bookmaker]:
                    self.bookmakers[bookmaker][eventid] = []
                self.bookmakers[bookmaker][eventid].append(outcome)
        logging.debug(f"Bookmakers after update: {self.bookmakers}")  # Debug print
        logging.debug(f"Number of bookmakers: {len(self.bookmakers)}")  # Debug print
        # Check if the market has enough data
        if self.has_enough_data():
            logging.info("Market has enough data. Proceeding to compare outcomes.")
            # Call compare_outcomes method
            #self.compare_outcomes(self.bookmakers, self.sharpbookkey, eventid)
            self.compare_averages()
            self.compared_games_count += 1  # Increment the counter
            logging.info(f"Number of games/events compared so far: {self.compared_games_count}")
            #self.average()
            #self.devig
            #self.more calculations
            #maybe a results array that is live streamed?
            
    def decereal(self, market_data, eventid, bookmaker):
        """
        Process the market data and return a dictionary with key, last_update, and outcomes.
        
        Args:
            market_data (dict): The market data to process. It's a dictionary with keys 'key', 'last_update', and 'outcomes'.
                                'outcomes' is a list of dictionaries with keys 'name', 'description', 'price', 'point'.
            eventid (str): The id of the event.
            bookmaker (str): The name of the bookmaker.
            
        Returns:
            dict: The processed market data. It's a dictionary with keys 'key', 'last_update', and 'outcomes'.
                  'outcomes' is a list of dictionaries with keys 'name', 'description', 'price', 'point', 'eventid', 'bookmaker'.
        """
        logging.debug(f"Decereal method called with eventid: {eventid}, bookmaker: {bookmaker}.")  
        try:
            key = market_data.get('key')
            last_update = market_data.get('last_update')
            outcomes_data = market_data.get('outcomes', [])

            if not outcomes_data:  # Check if outcomes_data is empty
                logging.warning(f"No outcomes data for eventid: {eventid}, bookmaker: {bookmaker}. Returning None.")
                return None

            outcomes = []
            for outcome in outcomes_data:
                outcomes.append({
                    'name': outcome.get('name'),
                    'description': outcome.get('description', ''),
                    'price': outcome.get('price'),
                    'point': outcome.get('point', None),
                    'eventid': eventid,  # Add the eventid to each outcome
                    'bookmaker': bookmaker  # Add the bookmaker to each outcome
                })
                logging.debug(f"Processed outcome for eventid: {eventid}, bookmaker: {bookmaker}. Outcome: {outcomes[-1]}")

            logging.debug(f"Finished processing outcomes for eventid: {eventid}, bookmaker: {bookmaker}. Total outcomes: {len(outcomes)}")

            return {
                'key': key,
                'last_update': last_update,
                'outcomes': outcomes
            }
        except Exception as e:
            logging.error(f"Error in decereal for eventid: {eventid}, bookmaker: {bookmaker}, error: {e}")
            return None
    
        
    def compare_averages(self):
        """
        Calculate and compare the average price of unique outcomes across all bookmakers and events,
        separately for bookmakers identified by sharpbookkey and the rest of the bookmakers.

        Returns:
            dict: A dictionary where each key is a unique outcome identifier and the value is a tuple containing the difference between the average price for bookmakers identified by sharpbookkey and the rest of the bookmakers.
        """
        logging.debug("Start comparing averages.")
        # Calculate the average price for each group of bookmakers concurrently
        sharpbookkey_average, basebook_average = self.average()
        logging.debug(f"Sharpbookkey average: {sharpbookkey_average}, Basebook average: {basebook_average}")

        # Initialize a dictionary to store the difference between the two averages for each unique outcome
        differences = {}

        # Calculate the difference between the two averages for each unique outcome
        for identifier in set(sharpbookkey_average.keys()).union(basebook_average.keys()):
            sharpbookkey_price = sharpbookkey_average.get(identifier)
            basebook_price = basebook_average.get(identifier)
            if sharpbookkey_price is not None and basebook_price is not None:
                differences[identifier] = sharpbookkey_price - basebook_price
        logging.debug(f"Differences: {differences}")

        return differences

    def calculate_average(self, bookmakers):
        """
        Calculate and return the average price of unique outcomes for the given bookmakers.

        Args:
            bookmakers (dict): A dictionary where each key is a bookmaker's name and the value is another dictionary.
                            The inner dictionary's keys are event ids and the values are lists of outcomes.
                            Each outcome is a dictionary with keys 'name', 'description', 'price', 'point', 'eventid', 'bookmaker'.

        Returns:
            dict: A dictionary where each key is a unique outcome identifier and the value is the average price.
        """
        logging.debug("Start calculating average.")
        # Initialize dictionaries to store the total price and count for each unique outcome
        totals = {}
        counts = {}

        for bookmaker, events in bookmakers.items():
            for eventid, outcomes in events.items():
                for outcome in outcomes:
                    # Create a tuple that uniquely identifies this outcome
                    identifier = (outcome['name'], outcome['description'], outcome['point'], outcome['eventid'])

                    # Update the total price and count for this outcome
                    totals[identifier] = totals.get(identifier, 0) + outcome['price']
                    counts[identifier] = counts.get(identifier, 0) + 1

        # Calculate the average price for each unique outcome
        outcome_averages = {identifier: totals[identifier] / counts[identifier] for identifier in totals}
        logging.debug(f"Outcome averages: {outcome_averages}")

        return outcome_averages

    def average(self):
        """
        Calculate and return the average price of unique outcomes across all bookmakers and events,
        separately for bookmakers identified by sharpbookkey and the rest of the bookmakers.

        Returns:
            tuple: A tuple containing two dictionaries. The first dictionary contains the average price for bookmakers identified by sharpbookkey.
                The second dictionary contains the average price for the rest of the bookmakers.
        """
        logging.debug("Start calculating average for all bookmakers.")
        # Separate the bookmakers into two groups
        sharpbookkey_bookmakers = {bookmaker: events for bookmaker, events in self.bookmakers.items() if bookmaker in self.sharpbookkey}
        basebook_bookmakers = {bookmaker: events for bookmaker, events in self.bookmakers.items() if bookmaker not in self.sharpbookkey}

        # Calculate the average price for each group of bookmakers concurrently
        sharpbookkey_average = self.calculate_average(sharpbookkey_bookmakers)
        basebook_average = self.calculate_average(basebook_bookmakers)
        logging.debug(f"Sharpbookkey average: {sharpbookkey_average}, Basebook average: {basebook_average}")

        return sharpbookkey_average, basebook_average
            
    @staticmethod
    def american_to_decimal(american_odds):
        if american_odds > 0:
            return 1 + (american_odds / 100)
        else:
            return 1 - (100 / american_odds)

    @staticmethod
    def decimal_to_mult_implied_probability(decimal_odds):
        return 1 / decimal_odds
    
    @staticmethod
    def fractional_to_add_implied_probability(fractional_odds):
        return 1/(fractional_odds+1)
    @staticmethod
    def decimal_to_fractional(decimal_percentage):
        decimal_odds = 1 / decimal_percentage
        numerator = round((decimal_odds - 1) * 100)
        denominator = 100
        return f"{numerator}/{denominator}"

    @staticmethod
    def remove_vig(implied_probabilities):
        total_probability = sum(implied_probabilities)
        return [prob / total_probability for prob in implied_probabilities]


class MarketManager:
    def __init__(self,min_bookmakers):
        self.market_objects = {}  # Stores market name: market object
        self.queue = asyncio.Queue()  # Queue for data
        self.done_event = asyncio.Event()  # Event
        self.min_bookmakers=min_bookmakers
        logging.debug(f"MarketManager object initialized with {self.min_bookmakers} bookmakers required")

    def get_market(self, name):
        """
        Retrieves the market object for a given name, creates it if it doesn't exist.

        Args:
            name (str): The name of the market.

        Returns:
            Market: The Market object for the given name.
        """
        # Get the market object for a given name, create it if it doesn't exist
        if name not in self.market_objects:
            self.market_objects[name] = Market(name,name,self.min_bookmakers)
            logging.debug(f"Created market object for {name}. Total market objects: {len(self.market_objects)}")
        else:
            logging.debug(f"Market object for {name} already exists. Total market objects: {len(self.market_objects)}")
        return self.market_objects[name]
    
    async def update_market_data(self):
        """
        Updates market data from the queue continuously.
        """
        logging.debug("Starting to update market data from the queue.")
        while True:
            logging.debug("Retrieving game market data from the queue.")
            game_market_data = await self.queue.get()
            if game_market_data is None:  # Break the loop if the sentinel value is retrieved
                logging.debug("Sentinel value retrieved from the queue. Breaking the loop.")
                break

            logging.debug("Checking if game market data is valid.")
            if game_market_data is None or 'bookmakers' not in game_market_data:
                logging.warning("Invalid game market data. Skipping this data.")
                continue

            logging.debug("Retrieving bookmakers data from game market data.")
            bookmakers_data = game_market_data.get('bookmakers', [])
            if not bookmakers_data:  # Check if bookmakers_data is empty
                logging.warning("No bookmakers data in game market data. Skipping this data.")
                continue

            logging.debug("Checking if game market data has all required bookmakers.")
            data_bookmakers = {bookmaker['key'] for bookmaker in bookmakers_data}
            missing_bookmakers = set(bookmakers) - data_bookmakers
            if missing_bookmakers:
                logging.warning(f"Data does not have all required bookmakers. Missing bookmakers: {', '.join(missing_bookmakers)}. Skipping this data.")
                continue

            logging.debug("Processing bookmakers data.")
            for bookmaker_data in bookmakers_data:
                logging.debug(f"Processing bookmaker data: {bookmaker_data}")
                if not isinstance(bookmaker_data, dict):  # Check if bookmaker_data is a dictionary
                    logging.error(f"Unexpected data type for bookmaker_data: {type(bookmaker_data)}. Skipping this data.")
                    continue

                logging.debug("Retrieving markets data from bookmaker data.")
                markets_data = bookmaker_data.get('markets', [])
                if not markets_data:  # Check if markets_data is empty
                    logging.warning("No markets data in bookmaker data. Skipping this data.")
                    continue

                logging.debug("Processing markets data.")
                for market_data in markets_data:
                    logging.debug(f"Processing market data: {market_data}")
                    if not isinstance(market_data, dict):  # Check if market_data is a dictionary
                        logging.error(f"Unexpected data type for market_data: {type(market_data)}. Skipping this data.")
                        continue

                    logging.debug("Updating market data.")
                    market = self.get_market(market_data['key'])
                    market.update_market_data(market_data, game_market_data['id'], bookmaker_data['key'])

                    logging.debug("Validating market data.")
                    if not market.validate_data(game_market_data):
                        logging.error(f"Data in Market object does not match original game data for market {market.name}.")
                    else:
                        logging.info(f"Valid data for market {market.name}. Data matches original game data.")

            logging.debug("Checking if done event is set.")
            if self.done_event.is_set():  # Check if done_event is set
                logging.info("Done event is set. Breaking the loop.")
                break

            logging.debug("Sleeping for a bit before checking the queue again.")
            await asyncio.sleep(0.1)  # sleep for a bit before checking the queue again```
   
api_call_count = 0  # Global variable to count API calls
import json

async def fetch_data(session, url, api_keys, current_key_index=0, retry_count=0):

    """
    Fetch data from a given URL. If the response status is 429 or the message in the data dictionary contains 'quota', 
    retry the request with exponential backoff and swapping API keys.

    Args:
        session (aiohttp.ClientSession): The aiohttp session to use for the request.
        url (str): The URL to fetch data from.
        api_keys (list): The list of API keys to use for the request.
        current_key_index (int, optional): The index of the current API key in the list. Defaults to 0.
        retry_count (int, optional): The number of times the request has been retried. Defaults to 0.

    Returns:
        dict: The fetched data. It's a dictionary with keys depending on the fetched data.
    """

    global total_delay_time
    # Fetch data from a given URL
    try:

        async with session.get(url) as response:
            global api_call_count
            api_call_count += 1
            logging.debug(f"API call #{api_call_count} to {url}")
            data = await response.text()  # Get the response data as a string
            data_dict = json.loads(data)  # Convert the JSON string to a dictionary
            if response.status == 429 or ('message' in data_dict and 'quota' in data_dict['message']):
                if retry_count >= len(api_keys):  # Retry limit
                    logging.error(f"Too many requests to {url}. Giving up after {retry_count} retries.")
                    return None
                delay = 2 ** retry_count if retry_count > 0 else 0
                total_delay_time += delay
                logging.debug(f"Exponential backoff delay: {delay} seconds. Total delay time: {total_delay_time} seconds.")
                await asyncio.sleep(delay)  # exponential backoff
                # Swap API keys
                current_key_index = (current_key_index + 1) % len(api_keys)
                url = url.replace(api_keys[current_key_index-1], api_keys[current_key_index])  # Update the url with the new API key
                logging.debug(f"Swapping API keys. New key index: {current_key_index}. New URL: {url}")
                retry_count += 1
                return await fetch_data(session, url, api_keys, current_key_index, retry_count)
            else:
                # Extract the response headers
                remaining_requests = response.headers.get('x-requests-remaining')
                used_requests = response.headers.get('x-requests-used')
                logging.debug(f"Remaining requests: {remaining_requests}, Used requests: {used_requests}")
                return data_dict
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return None

async def fetch_and_update_market_data(session, game, market_manager, bookmakers, markets, api_keys, current_key_index=0):

    """
    Fetch and update market data for a given game.

    This function fetches data for each game, bookmaker, and market, and updates the market data accordingly. It uses the `fetch_data` function to fetch data from the API and the `MarketManager` object to update the market data.

    The `results` variable in the function contains the fetched data for each API call. Each item in the `results` list is a dictionary that contains information about a game, including its ID, sport key, commence time, home team, away team, and bookmakers. Each bookmaker is represented as a dictionary that includes the bookmaker's key, title, and markets. Each market is also a dictionary that contains the market's key, last update time, and outcomes. Each outcome is a dictionary that includes the outcome's name, description, price, and point.

    Args:
        session (aiohttp.ClientSession): The aiohttp session to use for the request.
        game (dict): The game to fetch and update market data for. It's a dictionary with keys depending on the game data.
        market_manager (MarketManager): The MarketManager object to use for updating market data.
        bookmakers (list): The list of bookmakers to fetch data for.
        markets (list): The list of markets to fetch data for.
        api_keys (list): The list of API keys to use for the request.
        current_key_index (int, optional): The index of the current API key in the list. Defaults to 0.
    """

    try:
        # Check if game is a dictionary
        if not isinstance(game, dict):
            logging.error(f"Unexpected data type for game: {type(game)}. Skipping this game.")
            return

        # # Check if 'message' is in game, which indicates an error
        # if 'message' in game:#TODO shouldnt skip, should exp retry
        #     logging.error(f"Error from API for game: {game['message']}. Skipping this game.")
        #     return
        
        logging.info(f"Bookmakers for game {game['id']}: {bookmakers}")
        game_bookmaker_keys = [bookmaker['key'] for bookmaker in game.get('bookmakers', [])]
        logging.info(f"Bookmaker keys for game {game['id']}: {game_bookmaker_keys}")
        #game contains default h2h market, if no data for h2h, prob no data for requested markets
        missing_bookmaker_keys = set(bookmakers).difference(set(game_bookmaker_keys))
        
        if missing_bookmaker_keys:
            logging.warning(f"Game: {game['id']} missing h2h data so we don't even need to check if they have the other actual markets. Missing bookmakers: {', '.join(missing_bookmaker_keys)}. Home team: {game['home_team']}, Away team: {game['away_team']}, Raw data: {game}")
            raise Exception(f"Game: {game['id']} missing h2h data so we don't even need to check if they have the other actual markets. Missing bookmakers: {', '.join(missing_bookmaker_keys)}. Home team: {game['home_team']}, Away team: {game['away_team']}, Raw data: {game}")
        if 'bookmakers' not in game or not game['bookmakers']:
            logging.warning(f"No bookmakers for game: {game['id']}. Skipping this game.")
            return
        
        fetch_tasks = []
        total_api_calls = (len(bookmakers) + 9) // 10 * ((len(markets) + 9) // 10)
        logging.debug(f"Total expected API calls for game {game['id']}: {total_api_calls}. This is due to {len(bookmakers)} bookmakers and {len(markets)} markets.")

        for i in range(0, len(bookmakers), 10):
            bookmaker_batch = bookmakers[i:i+10]
            for j in range(0, len(markets), 10):
                market_batch = markets[j:j+10]
                market_data_url = f"https://api.the-odds-api.com/v4/sports/{game['sport_key']}/events/{game['id']}/odds?apiKey={api_keys[current_key_index]}&regions=uk&markets={','.join(market_batch)}&dateFormat=iso&oddsFormat=decimal&bookmakers={','.join(bookmaker_batch)}"
                fetch_tasks.append(fetch_data(session, market_data_url, api_keys, current_key_index))
        fetched_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

        if fetched_results:  # Check if results is not empty
            logging.debug(f"Fetched results for game {game['id']}: {fetched_results}")
            data_added_flag = False
            for fetched_game_data in fetched_results:
                #logging.info(data)
                if fetched_game_data is None:
                    logging.info(f"No data for game: {game['id']}. Skipping this game.")
                    continue
                # Filter out bookmakers that don't have data WE ASK FOR ALL REQUIRED BOOKMAKERS AT ONCE, SO THEORITICALLY IT SHOULD EITHER BE GOOD OR NOT MAYBE CAN RETURN HERE IF NOT
                fetched_game_data['bookmakers'] = [bookmaker for bookmaker in fetched_game_data.get('bookmakers', []) if bookmaker.get('markets', [])]
                if not fetched_game_data['bookmakers']:
                    logging.info(f"No bookmakers with data for game: {game['id']}. Skipping this game.")
                    continue
                logging.debug(f"Data to add to queue for game {game['id']}: {fetched_game_data}")  # Debug print 
                await market_manager.queue.put(fetched_game_data)
                data_added_flag = True
            if not data_added_flag:
                logging.info(f"No data added for game: {game['id']}")
        else:
            logging.info(f"No results data for game: {game['id']}")

        logging.info(f"Task completed for game: {game['id']}")  # New print statement
    except Exception as e:
        logging.error(f"Error in fetch_and_update_market_data for game: {game['id']}, error: {e}")

async def main(sport, bookmakers, markets, api_key):
    """
    Main function to fetch and update market data for all games of a sport.
    """
    logging.info("Starting main function...")

    # Main function to fetch and update market data for all games of a sport
    api_keys = ['e69e8575a4544a37e671d7761ed886df',api_key,'ea2f6d92d31a0649d1153d647ccb27a3','3ac0494f1936188c6f83b5834cb4659a']
    games_not_processed = []  # List to store games that were not processed
    logging.debug("Initialized api_keys and games_not_processed list.")

    async with aiohttp.ClientSession() as session:
        try:
            games_url = f'https://api.the-odds-api.com/v4/sports/{sport}/odds?apiKey={api_key}&regions=us&oddsFormat=american&bookmakers={",".join(bookmakers)}'
            logging.debug(f"Constructed games_url: {games_url}")
            game_data_list = await fetch_data(session, games_url,api_keys)
            if game_data_list is not None:
                logging.info(f"Total number of games: {len(game_data_list)}")
                market_manager = MarketManager(len(bookmakers))
                logging.debug("Initialized MarketManager.")
                update_task = asyncio.create_task(market_manager.update_market_data())
                logging.debug("Created update_task.")
                fetch_tasks = [asyncio.create_task(asyncio.wait_for(fetch_and_update_market_data(session, game, market_manager, bookmakers, markets, api_keys), timeout=10)) for game in game_data_list]
                logging.debug(f"Created {len(fetch_tasks)} fetch_tasks.")
                for task in asyncio.as_completed(fetch_tasks):
                    try:
                        await task
                    except asyncio.TimeoutError:
                        logging.error(f"Task timed out for game: {task.get_name()}")  # Assuming you set the name of the task to the game id
                await market_manager.queue.put(None)
                logging.debug("Put None in market_manager queue.")
                await update_task  # Wait for the update task to complete
                logging.debug("Completed update_task.")

        except Exception as e:
            logging.error(f"Error in main: {e}")

        logging.info("Finished main function.")
        return market_manager

import time
if __name__ == "__main__":
    logging.info("Starting main execution...")
    start_time = time.monotonic()

    sport = "basketball_nba"
    bookmakers = ["fanduel","fliff"]
    markets = ["player_threes"]
    api_key = "90c07e21da6a02b05273e5052c2ffd8c"
    logging.info(f"Parameters set - Sport: {sport}, Bookmakers: {', '.join(bookmakers)}, Markets: {', '.join(markets)}, API Key: {api_key}")
    market_manager=asyncio.run(main(sport, bookmakers, markets, api_key))
    
    end_time = time.monotonic()
    total_runtime = end_time - start_time - total_delay_time

    #print(f"Total runtime excluding delays: {total_runtime} seconds")
    print(market_manager.market_objects.items())
    for market_name, market_object in market_manager.market_objects.items():
        print(f"Market Name: {market_name}")
        print(f"Market ID: {market_object.id}")
        print(f"Market Name: {market_object.name}")
        print(f"Bookmakers: {market_object.bookmakers}")

    print(f"Total runtime excluding delays: {total_runtime} seconds")
