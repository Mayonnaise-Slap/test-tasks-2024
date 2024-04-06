import pandas as pd


def get_best_performing_day(variant_name: str) -> None:
    """
    Get best performing day for each month for first time clients from variant name csv file
    :param variant_name: name of the csv file that must be located in directory './data_for_testing/'
    :return: writes best performing day for each month for
    first time clients from variant name csv file output.csv,
    sep=\t, index=False
    """

    df = pd.read_csv(f'data_for_testing/{variant_name}', delimiter='	', header=0)

    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.date  # we don't need the time for that task

    min_month = df['timestamp'].min()
    max_month = df['timestamp'].max()

    # associate each user with the date of their first action on the website
    grouping = df[
        ['userid', 'timestamp']
    ].groupby('userid').min().reset_index()

    first_day = pd.merge(grouping, df,
                         on=['userid', 'timestamp'],
                         how='left',
                         validate='one_to_many'
                         )

    max_first_per_day = first_day[
        first_day['action'] == 'confirmation'
        ].groupby(
        'timestamp'
    )['value'].sum()

    # pandas looses the datetime format for some reason, so I have to recast it here
    max_first_per_day.index = pd.to_datetime(max_first_per_day.index)

    dirty_output = max_first_per_day.resample('ME').max()

    min_mask = (dirty_output.index.year == min_month.year) & (dirty_output.index.month == min_month.month)
    max_mask = (dirty_output.index.year == max_month.year) & (dirty_output.index.month == max_month.month)

    exclude_mask = max_mask | min_mask

    # mask out first and last month as well as empty dates.
    # Here it is the safest place to do so
    filtered_data = dirty_output[~exclude_mask]
    filtered_data.dropna(inplace=True)

    # casting to not have trailing .0
    filtered_data = filtered_data.astype(int)
    filtered_data.sort_index(inplace=True, ascending=True)
    # I leaved index to be True because in my case timestamp is the index itself
    filtered_data.to_csv('output.csv',
                         sep='\t')


if __name__ == '__main__':
    # sample of the function usage
    get_best_performing_day(variant_name='variant55.csv')

