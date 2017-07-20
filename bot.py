import os
import time
import pandas as pd
from slackclient import SlackClient

from crawler import CrawlerController

# instantiate Slack & Twilio clients
slack_client = SlackClient(os.environ.get('SLACK_BOT_TOKEN'))

BOT_CHANNEL = os.environ.get('SLACK_BOT_CHANNEL')
STRATEGY_FILE = 'strategy.csv'
READ_WEBSOCKET_DELAY = 1


def monitoring():
    with open(STRATEGY_FILE) as f:
        strategy = pd.read_csv(f, dtype={'symbol': object}).set_index('symbol')
        controller = CrawlerController(strategy.index)
        data = controller.run()
        try:
            df = pd.DataFrame(data).set_index('c')
        except:
            slack_client.api_call(
                'chat.postMessage',
                channel=BOT_CHANNEL,
                text='Cannot fetch data!!',
            )
            return
        df = pd.merge(
            df, strategy,
            left_index=True, right_index=True
        ).astype({'z': float})

        triggered_buy = df[df['z'] < df['low']]
        triggered_sell = df[df['z'] > df['high']]

        for symbol in strategy.loc[triggered_buy.index][strategy['triggered'] != 1].index:
            slack_client.api_call(
                'chat.postMessage',
                channel=BOT_CHANNEL,
                text='Should buy %s now!!' % symbol,
            )

        for symbol in strategy.loc[triggered_sell.index][strategy['triggered'] != -1].index:
            slack_client.api_call(
                'chat.postMessage',
                channel=BOT_CHANNEL,
                text='Should sell %s now!!' % symbol,
            )

        strategy['triggered'] = 0
        strategy.ix[triggered_buy.index, 'triggered'] = 1
        strategy.ix[triggered_sell.index, 'triggered'] = -1

    with open(STRATEGY_FILE, 'w') as f:
        strategy.to_csv(f)


def post_strategy():
    with open(STRATEGY_FILE) as f:
        slack_client.api_call(
            'chat.postMessage',
            channel=rtm['channel'],
            text=pd.read_csv(f, dtype={'symbol': object}).to_dict('records')
        )


def handle_message(message):
    if rtm['type'] == 'message' and 'subtype' not in rtm:
        text = rtm['text']
        command = text.split()[0]
        if command == 'set':
            symbol, low_high = text.split()[1:]
            low, high = low_high.split(',')
            with open(STRATEGY_FILE) as f:
                strategy = pd.read_csv(
                    f, dtype={'symbol': object}
                ).set_index('symbol')
                if low:
                    strategy.ix[symbol, 'low'] = float(low)
                if high:
                    strategy.ix[symbol, 'high'] = float(high)
            with open(STRATEGY_FILE, 'w') as f:
                strategy.to_csv(f)
        elif command == 'unset':
            symbol, price_type = text.split()[1:]
            with open(STRATEGY_FILE) as f:
                strategy = pd.read_csv(
                    f, dtype={'symbol': object}
                ).set_index('symbol')
                print(strategy)
                if price_type == 'low':
                    strategy.ix[symbol, 'low'] = None
                elif price_type == 'high':
                    strategy.ix[symbol, 'high'] = None
                elif price_type == 'all':
                    strategy.drop(symbol, inplace=True)
            with open(STRATEGY_FILE, 'w') as f:
                strategy.to_csv(f)
        post_strategy()


if __name__ == '__main__':
    if slack_client.rtm_connect():
        print('StarterBot connected and running!')
        t = 0
        while True:
            for rtm in slack_client.rtm_read():
                handle_message(rtm)
            if t == 0:
                monitoring()
            t += 1
            if t > 3:
                t = 0
            time.sleep(READ_WEBSOCKET_DELAY)
    else:
        print('Connection failed.')
