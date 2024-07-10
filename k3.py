import aiohttp
import asyncio
import logging
from cachetools import TTLCache
total_delay_time = 0
total_delay_time_lock = asyncio.Lock()
# Configure logging
logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
from scipy.optimize import fsolve

sharpbookkeys = {"player_assists": ["draftkings"], "player_threes": ["espnbet","fliff"]}

class Market:
    def __init__(self, id: str, name: str, min_bookmakers: int):
        """
        Initialize a new 'Market' object.
        
        Args:
            id (str): The unique identifier of the market.
            name (str): The descriptive name of the market.
            min_bookmakers (int): The minimum number of bookmakers required for the market.
        """
        global sharpbookkeys
        self.id = id
        self.name = name
        self.bookmakers = {}  # This will now store bookmaker: {outcome: data} //redo all as sets()? no dicts
        self.sharpbookkey = sharpbookkeys.get(name, ['draftkings'])  # update with flag marked from main()
        logging.debug(f"SHARPBOOKKEY FOR {self.name}: {self.sharpbookkey}")
        self.MIN_BOOKMAKERS=2
        self.compared_games_count = 0
        self.power_devig_cache = TTLCache(maxsize=1000, ttl=300)  # Cache for power_devig calculation//autoaverage
        self.synthetic_market_info = {} 
        self.results = {} 


    def calculate_and_emit_outcomes(self):
        
        # Placeholder for a method to calculate and emit outcomes
        logging.debug("calculate_and_emit_outcomes method called.")
        
    def validate_data(self, game_data):
        """
        Validates the game data against the Market object's bookmakers data.
        
        This method checks if the bookmakers and events in the game data match the bookmakers and events in the Market object.
        If there is a mismatch, it logs a warning and returns False. If the data matches, it returns True.
        
        Args:
            game_data (dict): The game data to validate. It's a dictionary with keys 'bookmakers' and 'events'.
                                'bookmakers' is a list of dictionaries with key 'key'.
                                'events' is a list of dictionaries with key 'id'.
        
        Returns:
            bool: True if the game data matches the Market object's data, False otherwise.
        """
        logging.debug("Starting data validation.")
        
        # Extract the bookmakers and events from the game data
        game_bookmakers = [bookmaker['key'] for bookmaker in game_data.get('bookmakers', [])]
        game_events = [event['id'] for event in game_data.get('events', [])]
        
        # Extract the bookmakers from the Market object
        market_bookmakers = list(self.bookmakers.keys())

        logging.debug(f"Game bookmakers: {game_bookmakers}")
        logging.debug(f"Market bookmakers: {market_bookmakers}")
        logging.debug(f"Game data: {game_data}")
        logging.debug(f"Market object bookmakers data: {self.bookmakers}")

        # Check if the bookmakers in the game data match the bookmakers in the Market object
        if set(game_bookmakers) != set(market_bookmakers):
            logging.warning(f"Bookmakers in Market object do not match game data for market {self.name}.")
            logging.warning(f"Difference: {set(game_bookmakers).symmetric_difference(set(market_bookmakers))}")
            logging.warning(f"Similarity: {set(game_bookmakers).intersection(set(market_bookmakers))}")
            return False

        # Check if the events in the game data match the events in the Market object
        for bookmaker, outcomes in self.bookmakers.items():
            for outcome_key in outcomes:
                eventid = outcome_key[0]
                if eventid not in game_events:
                    logging.warning(f"Event {eventid} in Market object do not match game data for market {self.name}.")
                    return False

        logging.debug("Data validation completed successfully.")
        return True
        
    def get_opposite_name(self,outcomename):
        """
        Returns the opposite of a given outcome name.

        Args:
            outcomename (str): The outcomename to find the opposite of. 
                            It can be "over", "under", "yes", or "no".

        Returns:
            str: The opposite of the given outcomename, or None if the outcomename is not recognized.
        """
        if outcomename == "over":
            return "under"
        elif outcomename == "under":
            return "over"
        elif outcomename == "yes":
            return "no"
        elif outcomename == "no":
            return "yes"
        else:
            return None

    def update_market_data(self, market_data, eventid, bookmaker): 
        """
        Update the market data for each bookmaker. The market data is stored in the self.bookmakers dictionary.
        The structure of self.bookmakers is as follows:
        self.bookmakers = {bookmaker: {(eventid, outcome_name, outcome_description, outcome_point): {bookmaker: outcome_price}}}
        
        Args:
            market_data (dict): The market data to update. It's a dictionary with keys 'key', 'last_update', and 'outcomes'.
                                'outcomes' is a list of dictionaries with keys 'name', 'description', 'price', 'point'.
            eventid (str): The id of the event.
            bookmaker (str): The name of the bookmaker.
        """
        logging.info(f"Starting to update market data for bookmaker {bookmaker} and event {eventid} with market data: {market_data}")
        decereal_data = self.decereal(market_data, eventid, bookmaker)
        logging.info(f'Decereal data: {decereal_data}')
        if decereal_data is not None and decereal_data['outcomes'] is not None:
            for outcome in decereal_data['outcomes']:
                # Define the unique key for the outcome
                outcome_key = (eventid, outcome['name'], outcome['description'], outcome['point'])
                # Add the outcome to the dictionary, using the unique key
                if bookmaker not in self.bookmakers:
                    self.bookmakers[bookmaker] = {}
                if outcome_key not in self.bookmakers[bookmaker]:
                    self.bookmakers[bookmaker][outcome_key] = {}
                self.bookmakers[bookmaker][outcome_key][bookmaker] = outcome['price']
                logging.debug(f"Updated bookmaker {bookmaker} data with outcome {outcome_key} and price {outcome['price']}")
                # If the bookmaker is in sharpbookkey, check if the opposite outcome exists and perform the calculations
                if bookmaker in self.sharpbookkey:
                    logging.info(f"Bookmaker {bookmaker} is in sharpbookkey. Proceeding with calculations.")
                    opposite_name = self.get_opposite_name(outcome['name'])
                    logging.debug(f"Opposite name for outcome {outcome['name']} is {opposite_name}")
                    opposite_key = (eventid, opposite_name, outcome['description'], outcome['point'])
                    logging.debug(f"Opposite key for outcome {outcome_key} is {opposite_key}")
                    if opposite_key in self.bookmakers[bookmaker]:
                        logging.info(f"Opposite outcome {opposite_key} exists. Proceeding with calculations.")
                        # If the opposite outcome exists, perform the calculations
                        price1 = self.bookmakers[bookmaker][outcome_key][bookmaker]
                        price2 = self.bookmakers[bookmaker][opposite_key][bookmaker]
                        logging.debug(f"Prices for outcome {outcome_key} and opposite outcome {opposite_key} are {price1} and {price2} respectively.")
                        results = self.calculate(price1, price2)  # Replace with your actual calculation method
                        logging.debug(f"Calculated results for outcome {outcome_key} and opposite outcome {opposite_key} are {results}")
                        # Store the result for averaging later
                        # logging.debug(f"RESULTS:{self.results}")
                        logging.debug(f"Outcome key: {outcome_key}, Bookmaker: {bookmaker}")
                        logging.debug(f"Current keys in self.results: {self.results.keys()}")
                        if outcome_key not in self.results:
                            self.results[outcome_key] = {calculation: {'total1': result['newover'], 'total2': result['newunder'], 'count': 1, 'bookmakers': [bookmaker]} for calculation, result in results.items()}
                            logging.info(f"Stored initial results for outcome {outcome_key}.")
                        else:
                            logging.info(f"Outcome key {outcome_key} already exists in results. Updating existing data.")
                            for calculation, result in results.items():
                                self.results[outcome_key][calculation]['total1'] += result['newover']
                                self.results[outcome_key][calculation]['total2'] += result['newunder']
                                self.results[outcome_key][calculation]['count'] += 1
                                self.results[outcome_key][calculation]['bookmakers'].append(bookmaker)
                                logging.debug(f"Updated results for outcome {outcome_key} with new data.")

                                # Calculate the average right after updating total1 and total2
                                self.results[outcome_key][calculation]['total1'] /= self.results[outcome_key][calculation]['count']
                                self.results[outcome_key][calculation]['total2'] /= self.results[outcome_key][calculation]['count']
                                logging.debug(f"New total1: {self.results[outcome_key][calculation]['total1']}, New total2: {self.results[outcome_key][calculation]['total2']}")
                            # for calculation, result in results.items():
                            #     self.results[outcome_key][calculation]['total1'] += result['newover']
                            #     self.results[outcome_key][calculation]['total2'] += result['newunder']
                            #     self.results[outcome_key][calculation]['count'] += 1
                            #     self.results[outcome_key][calculation]['bookmakers'].append(bookmaker)
                            #     logging.debug(f"Updated results for outcome {outcome_key} with new data.")
                            # for calculation in results:
                            #     old_total1 = self.results[outcome_key][calculation]['total1']
                            #     old_total2 = self.results[outcome_key][calculation]['total2']
                            #     old_count = self.results[outcome_key][calculation]['count']
                            #     logging.debug(f"Old total1: {old_total1}, Old total2: {old_total2}, Old count: {old_count}")
                            #     self.results[outcome_key][calculation]['total1'] = old_total1 / old_count
                            #     self.results[outcome_key][calculation]['total2'] = old_total2 / old_count
                            #     logging.debug(f"New total1: {self.results[outcome_key][calculation]['total1']}, New total2: {self.results[outcome_key][calculation]['total2']}")
                            # logging.debug(f"Updated results for outcome {outcome_key} with averages {self.results[outcome_key]}")
                # If the bookmaker is not in sharpbookkey, compare the outcome with the relevant part of the outcome pair
                else:
                    logging.info(f"Bookmaker {bookmaker} is not in sharpbookkey. Proceeding with comparison.")
                    relevant_outcome_key = (eventid, outcome['name'], outcome['description'], outcome['point'])
                    logging.debug(f"Relevant outcome key for comparison is {relevant_outcome_key}")
                    if relevant_outcome_key in self.results: #TODO shouldnt it be for each devig method's averages.
                        logging.info(f"Relevant outcome {relevant_outcome_key} exists in results. Proceeding with comparison.")
                        relevant_results = {calculation: {'total1': result['total1'], 'total2': result['total2']} for calculation, result in self.results[relevant_outcome_key].items()}
                        # Perform the comparison
                        logging.debug(f'Comparing {outcome["name"], outcome["description"], outcome["point"]} with {relevant_outcome_key}')
                        #self.compare(outcome['price'], relevant_result)  # Replace with your actual comparison method

    def calculate(self, price1, price2):
        """
        Perform multiple independent calculations on price1 and price2.

        Args:
            price1 (float): The price of the first outcome.
            price2 (float): The price of the second outcome.

        Returns:
            dict: A dictionary where each key is the name of the independent calculation and the value is a tuple of the new price1 and price2.
        """
        # Perform your independent calculations here
        powerdevigresult=self.power_devig(price1,price2)
        power_devig = {'newover':1,'newunder':1}
        multdevigresult=self.mult_devig(price1,price2)
        
        mult_devig = {'newover': multdevigresult[0], 'newunder': multdevigresult[1]}
        print(mult_devig)
        add_devig = {'newover':1,'newunder':1}

        return {
            'power_devig': power_devig,
            'mult_devig': mult_devig,
            'add_devig': add_devig
        }

    def compare_and_emit_outcomes(self, relevant_outcomes, nonsharp_outcomes):
        """
        Compare the outcomes from the non-sharpbookmaker with the relevant outcomes from the synthetic market and emit the outcomes.

        Args:
            relevant_outcomes (dict): A dictionary where each key is a tuple of eventid and description, and the value is a list of relevant outcomes.
            nonsharp_outcomes (list): A list of outcomes from the non-sharpbookmaker.
        """
        pass
        for nonsharp_outcome in nonsharp_outcomes:
            identifier = (nonsharp_outcome['eventid'], nonsharp_outcome['description'])
            if identifier in relevant_outcomes:
                for relevant_outcome in relevant_outcomes[identifier]:
                    synthetic_market_info = self.synthetic_market_info.get(relevant_outcome['identifier'])
                    if synthetic_market_info:
                        # Compare nonsharp_outcome with synthetic_market_info
                        # Emit the outcomes as needed
                        pass
    

    def mult_devig(self, price1, price2):
        """
        Calculate and return the probabilities based on the given prices.

        Args:
            price1 (float): The price of the first outcome.
            price2 (float): The price of the second outcome.

        Returns:
            tuple: A tuple containing the calculated probabilities.
        """
        # Calculate implied probabilities
        compoverimplied = 1 / price1
        compunderimplied = 1 / price2

        # Calculate actual probabilities
        actualoverdecimal = compoverimplied / (compoverimplied + compunderimplied)
        actualunderdecimal = compunderimplied / (compunderimplied + compoverimplied)

        return actualoverdecimal, actualunderdecimal
    
    
    def power_devig(self,price1, price2): #price1 &2 should be decimal format not american
        """
        Calculate and return the probabilities based on the given prices.

        Args:
            price1 (float): The price of the first outcome.
            price2 (float): The price of the second outcome.

        Returns:
            tuple: A tuple containing the calculated probabilities and American odds.
        """
        # Calculate implied probabilities
        compoverimplied = 1 / price1
        compunderimplied = 1 / price2
        
        # Define the function to solve for k
        def f(k):
            ri1 = compoverimplied
            ri2 = compunderimplied
            return ri1**(1/k) + ri2**(1/k) - 1

        # Solve for k
        k_initial_guess = 1
        k_solution = fsolve(f, k_initial_guess)

        pi1 = compoverimplied**(1/k_solution[0])
        pi2 = compunderimplied**(1/k_solution[0])
        logging.debug(f"Calculated probabilities: {pi1}, {pi2}")
        # Convert probabilities to Decimal odds
        actualoverdecimal = 1 / pi1
        actualunderdecimal = 1 / pi2

        return actualoverdecimal,actualunderdecimal
    

    def sanitize_string(self, string):
        """
        Sanitize a string to be uniform.

        Args:
            string (str): The string to sanitize.

        Returns:
            str: The sanitized string.
        """
        # Implement your sanitization logic here
        sanitized_string = string.lower().strip()
        return sanitized_string
            
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
                name = self.sanitize_string(outcome.get('name'))
                description = self.sanitize_string(outcome.get('description', ''))
            
                outcomes.append({
                    'name': name,
                    'description': description,
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

            # logging.debug("Checking if game market data has all required bookmakers.")
            # data_bookmakers = {bookmaker['key'] for bookmaker in bookmakers_data}
            # missing_bookmakers = set(bookmakers) - data_bookmakers
            # if missing_bookmakers:
            #     logging.warning(f"Data does not have all required bookmakers. Missing bookmakers: {', '.join(missing_bookmakers)}. Skipping this data.")
            #     continue

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
                await asyncio.sleep(delay)  # exponential backoff
                async with total_delay_time_lock:
                    global total_delay_time
                    total_delay_time += delay
                logging.debug(f"Exponential backoff delay: {delay} seconds. Total delay time: {total_delay_time} seconds.")
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

        logging.info(f"Bookmakers for game {game['id']}: {bookmakers}")
        game_bookmaker_keys = [bookmaker['key'] for bookmaker in game.get('bookmakers', [])]
        logging.info(f"Bookmaker keys for game {game['id']}: {game_bookmaker_keys}")

        # Check if there is at least one sharpbookmaker and one other bookmaker for each market
        for market in markets:
            sharpbookmaker_keys = set(bookmakers).intersection(set(sharpbookkeys.get(market, [])))
            other_bookmaker_keys = set(bookmakers).difference(set(sharpbookkeys.get(market, [])))
            if not (sharpbookmaker_keys.intersection(game_bookmaker_keys) and other_bookmaker_keys.intersection(game_bookmaker_keys)):
                logging.warning(f"Game: {game['id']} does not have at least one sharpbookmaker and one other bookmaker for market {market}. Skipping this game.")
                return

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
    markets = ["player_threes"]
    api_key = "90c07e21da6a02b05273e5052c2ffd8c"
    base_bookmakers=["fanduel"]
    # Automatically include all the relevant bookmakers based on the requested market
    bookmakers = base_bookmakers[:]
    for market in markets:
        if market in sharpbookkeys:
            bookmakers.extend(sharpbookkeys[market])
    bookmakers = list(set(bookmakers))  # Remove duplicates

    logging.info(f"Parameters set - Sport: {sport}, Bookmakers: {', '.join(bookmakers)}, Markets: {', '.join(markets)}, API Key: {api_key}")
    market_manager=asyncio.run(main(sport, bookmakers, markets, api_key))
    
    end_time = time.monotonic()
    total_runtime = end_time - start_time - total_delay_time

    print(market_manager.market_objects.items())
    for market_name, market_object in market_manager.market_objects.items():
        print(f"Market Name: {market_name}")
        print(f"Market ID: {market_object.id}")
        print(f"Market Name: {market_object.name}")
        print(f"Bookmakers: {market_object.bookmakers}")

    print(f"Total runtime excluding delays: {total_runtime} seconds")