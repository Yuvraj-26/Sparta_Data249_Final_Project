import pandas as pd


def behaviours_junction_table(academy_df, talent_df, behaviours_df):
    """
    Takes the transformed academy_df, transformed talent_df and behaviours_df (in that order) and returns the
    behaviours junction table with all IDs input correctly
    """
    weeks_and_behaviours = academy_df.columns[4:]
    titles = []
    for title in weeks_and_behaviours:
        title = title.split('_w')
        titles.append(title)

    weeks = []
    behaviour = []
    for each in titles:
        weeks.append(each[1])
        behaviour.append(each[0])

    scores = academy_df.iloc[:, 4:]
    scores = scores.values.tolist()

    junction_table = pd.DataFrame(columns=['first_name', 'last_name', 'behaviour', 'week', 'score'])
    for x in range(len(scores)):
        first = academy_df.iloc[x, 0]
        last = academy_df.iloc[x, 1]
        data = {'first_name': first, 'last_name': last, 'behaviour': behaviour, 'week': weeks, 'score': scores[x]}
        df = pd.DataFrame(data)
        junction_table = pd.concat([junction_table, df])

    output = junction_table.merge(behaviours_df, how='left', left_on='behaviour', right_on='behaviours')
    output = output.drop('behaviour', axis=1)
    output = output.drop('behaviours', axis=1)

    talent_df = talent_df.reset_index(drop=True)
    talent_df['talent_id'] = talent_df.index + 1
    desired_order = ['talent_id', 'first_name', 'last_name', ]
    talent_df = talent_df[desired_order]

    output = output.merge(talent_df, how='left', left_on=['first_name', 'last_name'],
                          right_on=['first_name', 'last_name'])
    output = output.drop('first_name', axis=1)
    output = output.drop('last_name', axis=1)
    output = output[['talent_id', 'behaviour_id', 'week', 'score']]
    # print(output)
    return output
