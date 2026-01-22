'''
Docstring for feed_functions.feedcost_sequences
'''
import pandas as pd

class FeedCostSequences:
    @staticmethod
    def create(feed_name, feed_series_dict, groups_count, herd_daily, price_seq, date_range):
        daily_amt = feed_series_dict[feed_name]['dad']
        herd_daily_dry = herd_daily[['dry']]
        daily_amt = pd.concat([daily_amt, groups_count, herd_daily_dry], axis=1).fillna(0)
        daily_amt['fresh_amt']   = daily_amt['fresh_kg']      * daily_amt['fresh']
        daily_amt['group_a_amt'] = daily_amt['group_a_kg']    * daily_amt['groupA']
        daily_amt['group_b_amt'] = daily_amt['group_b_kg']    * daily_amt['groupB']
        daily_amt['dry_amt']     = daily_amt['dry_kg']        * daily_amt['dry']
        daily_amt['total_amt'] = daily_amt[['fresh_amt','group_a_amt','group_b_amt','dry_amt']].sum(axis=1)
        daily_price_seq = price_seq.reindex(date_range).ffill()
        cost_sequence = pd.concat((daily_amt, daily_price_seq), axis=1)
        cost_sequence['daily cost'] = cost_sequence['unit_price'] * cost_sequence['total_amt']
        return daily_amt, daily_price_seq, cost_sequence
