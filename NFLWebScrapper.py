# BY Claudio Mazzoni, October 23, 2017
# Web Scrapper functions for website: http://www.footballdb.com/
import bs4
import re
import pandas as pd
from dateutil import parser
import urllib.request
import urllib.error


def web_srapper_nfl_games():

    years = ['1984', '1985', '1986', '1987', '1988', '1989', '1990', '1991', '1992', '1993', '1994',
             '1995', '1996', '1997', '1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006',
             '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017']

    cols = ['Date', 'Week', 'Team Home', 'Team Away', 'Final Home', 'Final Away', 'Victory', '1H', '2H',
            '3H', '4H', '1A', '2A', '3A', '4A', 'Home Team First downs', 'Home Team Passing',
            'Home Team Total Net Yards', 'Home Team Rushing Plays', 'Home Team Net Yards Passing',
            'Home Team Sacked - Yds Lost', 'Home Team Avg. Yds/Att', 'Home Team Had Blocked',
            'Home Team Kickoff Returns', 'Home Team Penalties - Yards', 'Home Team Field Goals',
            'Home Team Fourth Downs', 'Home Team Average Gain', 'Home Team Rushing', 'Home Team Penalty',
            'Home Team Net Yards Rushing', 'Home Team Att - Comp - Int', 'Home Team Gross Yards Passing',
            'Home Team Punts - Average', 'Home Team Punt Returns', 'Home Team Interception Returns',
            'Home Team Fumbles - Lost', 'Home Team Third Downs', 'Home Team Total Plays',
            'Home Team Time of Possession', 'Away Team First downs', 'Away Team Passing',
            'Away Team Total Net Yards', 'Away Team Rushing Plays', 'Away Team Net Yards Passing',
            'Away Team Sacked - Yds Lost', 'Away Team Avg. Yds/Att', 'Away Team Had Blocked',
            'Away Team Kickoff Returns', 'Away Team Penalties - Yards', 'Away Team Field Goals',
            'Away Team Fourth Downs', 'Away Team Average Gain', 'Away Team Rushing', 'Away Team Penalty',
            'Away Team Net Yards Rushing', 'Away Team Att - Comp - Int', 'Away Team Gross Yards Passing',
            'Away Team Punts - Average', 'Away Team Punt Returns', 'Away Team Interception Returns',
            'Away Team Fumbles - Lost', 'Away Team Third Downs', 'Away Team Total Plays',
            'Away Team Time of Possession', 'tmp']

    weeks = range(1, 22)
    df = pd.DataFrame(
        columns=cols)

    for yrs in years:
        for week in weeks:
            if week < 18:
                web_nfl = 'http://www.footballdb.com/scores/index.html?lg=NFL&yr={0}&type=reg&wk={1}'\
                    .format(yrs, str(week))
            else:
                web_nfl = 'http://www.footballdb.com/scores/index.html?lg=NFL&yr={0}&type=post&wk={1}' \
                    .format(yrs, str(week - 17))
            try:
                page = urllib.request.urlopen(web_nfl)
                soup = bs4.BeautifulSoup(page, 'html.parser')
                matches = soup.find_all('span', attrs={'class': 'hidden-xs'})
                date_box = soup.find_all('div', attrs={'class': 'divider'})
                scoreforeach = soup.find_all('tr', attrs={'class': 'row0 center'})
                name_dt = ''
                orders = 1
                for match in range(len(matches)):
                    if match % 2 == 0:
                        dat = 0
                        for dot in range(len(date_box)):
                            if matches[match] in date_box[dot].find_all_next('span', attrs={'class': 'hidden-xs'}):
                                dat = dot
                        if name_dt != date_box[dat].string:
                            orders = 1
                        else:
                            orders += 1
                        name_dt = date_box[dat].string
                        form_date = parser.parse(name_dt).strftime('%m/%d/%Y')
                        team_away = matches[match].string
                        scores_away = re.findall('\d+', str(scoreforeach[match]).splitlines()[2])
                        first_away = scores_away[0]
                        second_away = scores_away[1]
                        third_away = scores_away[2]
                        fourth_away = scores_away[3]
                        final_score_away = scores_away[4]
                        team_home = matches[match + 1].string
                        scores_home = re.findall('\d+', str(scoreforeach[match + 1]).splitlines()[2])
                        first_home = scores_home[0]
                        second_home = scores_home[1]
                        third_home = scores_home[2]
                        fourth_home = scores_home[3]
                        final_score_home = scores_home[4]
                        data = {'Date': form_date, 'Week': str(week), 'Team Home': team_home, 'Team Away': team_away,
                                'Final Home': final_score_home, 'Final Away': final_score_away, '1H': first_home,
                                '2H': second_home, '3H': third_home, '4H': fourth_home, '1A': first_away,
                                '2A': second_away, '3A': third_away, '4A': fourth_away}
                        if orders > 9:
                            str_topass = str(orders)
                        else:
                            str_topass = '0' + str(orders)
                        formpass = parser.parse(name_dt).strftime('%Y%m%d') + str_topass
                        boxes = scrapboxscore(formpass)
                        df2 = pd.DataFrame.from_dict(data, orient='index').transpose()
                        df2['tmp'] = 1
                        df2 = pd.merge(df2, boxes, how='left', on=['tmp'])
                        if df2['Final Home'][0] > df2['Final Away'][0]:
                            df2['Victory'] = 'H'
                        elif df2['Final Home'][0] < df2['Final Away'][0]:
                            df2['Victory'] = 'A'
                        else:
                            df2['Victory'] = 'D'
                        df = df.append(df2, ignore_index=True)

            except (IndexError, AssertionError):
                print('Games ended at: ', week)
                break
    df = df[cols]
    del df['tmp']
    df.to_csv('C:\\Users\\...\\Games_Hist_Data.csv')
    web_srapper_nfl_scores()


def scrapboxscore(boxer):

    web_nfl = 'http://www.footballdb.com/games/boxscore.html?gid={}'.format(boxer)
    page = urllib.request.urlopen(web_nfl)
    soup = bs4.BeautifulSoup(page, 'html.parser')
    matches = soup.find_all('table', attrs={'class': 'statistics'})
    loops = ['0', '1']
    col = []
    tagnunm = 0
    for lup in loops:
        for match in matches[2:4]:
            tagnunm += 1
            lis = match.find_all('tr', attrs={'class': 'row' + lup + ' center'})
            if len(lis) > 2:
                for l in lis:
                    clean1 = [b.text for b in l.find_all('td')]
                    col.extend(clean1)

    dicto_home = {'Home Team ' + col[i]: col[i + 2] for i in range(0, len(col), 3)}
    dicto_away = {'Away Team ' + col[i]: col[i + 1] for i in range(0, len(col), 3)}
    power_combo = {**dicto_home, **dicto_away}
    df1 = pd.DataFrame.from_dict(power_combo, orient='index').transpose()
    df1['Game Location'] = ''.join(soup.find_all('center')[1].find('div')
                                   .get_text(separator='\n').splitlines()[1].split(',')[-2:])
    df1['tmp'] = 1
    return df1


def web_srapper_nfl_scores():
    years = ['1984', '1985', '1986', '1987', '1988', '1989', '1990', '1991', '1992', '1993', '1994',
             '1995', '1996', '1997', '1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006',
             '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017']
    of_de = ['O', 'D']

    for do in of_de:
        df = pd.DataFrame(columns=['Year', 'Team', 'Pts/G', 'RYds/G', 'PYds/G', 'Yds/G'])
        for yrs in years:
            web_nfl = 'http://www.footballdb.com/stats/' \
                      'teamstat.html?lg=NFL&yr={0}&type=reg&cat=T&group={1}&conf='.format(yrs, do)
            page = urllib.request.urlopen(web_nfl)
            soup = bs4.BeautifulSoup(page, 'html.parser')
            matches = soup.find_all('tr', attrs={'class': 'header right'})
            matchins = matches[0].find_all('th')
            heads = [matchins[n].string for n in range(len(matchins))]
            heads.append('Year')
            webs = ['0', '1']
            for i in range(len(webs)):
                content = soup.find_all('tr', attrs={'class': 'row' + webs[i] + ' right'})  # good for values
                for r in range(len(content)):
                    valorr = content[r].find_all('td')
                    teams = valorr[0].find_all('span', attrs={'class': 'hidden-xs'})[0].string
                    valuer = [valorr[rarr].string if valorr[rarr].string is not None
                              else teams for rarr in range(len(valorr))]
                    valuer.append(yrs)
                    diccomp = dict(zip(heads, valuer))
                    cleancomp = {i: diccomp[i] for i in diccomp if '/G' in i or 'Team' in i or 'Year' in i}
                    df2 = pd.DataFrame.from_dict(cleancomp, orient='index').transpose()
                    df = df.append(df2, ignore_index=True)

        df = df[['Year', 'Team', 'Pts/G', 'RYds/G', 'PYds/G', 'Yds/G']]
        print(df)
        df.to_csv('C:\\Users\\...\\{}_Data.csv'.format(do))


def recon_files():
    years = ['1984', '1985', '1986', '1987', '1988', '1989', '1990', '1991', '1992', '1993', '1994',
             '1995', '1996', '1997', '1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006',
             '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017']
    teams = []
    df = pd.read_csv('C:\\Users\\..\.\Games_Hist_Data.csv')
    for index_val, series_val in df['Team Home'].iteritems():
        if series_val not in teams:
            teams.append(series_val)
    heads = ['Year', 'Team', 'Passing Comp', 'Passing Yds',
             'Passing TD', 'Passing Rate', 'Rushing Yds', 'Rushing TD',
             'Receiving Tot', 'Receiving Yds', 'Recieving TD',
             'KickoffR Tot', 'KickoffR Yds', 'KickoffR TD',
             'PuntsR Tot', 'PuntsR Yds', 'PuntsR TD', 'Punt Tot',
             'Punt Tot', 'Punt Yds', 'Kicking Pts']
    plyr_pos = pd.DataFrame()
    for yer in range(len(years)):
        for i in range(len(teams)):
            try:
                tm = teams[i].lower().replace(' ', '-').replace('.', '')
                statsurl = 'http://www.footballdb.com/teams/nfl/{0}/stats/{1}'.\
                    format(tm, years[yer])
                print(statsurl)
                print(teams[i])
                page = urllib.request.urlopen(statsurl)
            except urllib.error.HTTPError as err:
                print(err)
                continue

            soup = bs4.BeautifulSoup(page, 'html.parser')
            statters = {'P': [2, 4, 6, 13], 'R': [3, 7],
                        'C': [2, 3, 6], 'KR': [1, 2, 6], 'PR': [1, 2, 6],
                        'U': [1, 2], 'K': [9]}
            to_pd = [years[yer], teams[i]]
            try:
                for key, vals in statters.items():
                    dirt_soup = soup.find_all('div', attrs={'id': 'divToggle_{}'.format(key)})
                    if len(teams[i].split(' ')) > 2:
                        if teams[i].split(' ')[1] == 'York':
                            citay = 'NY ' + teams[i].split(' ')[2]
                        elif teams[i].split(' ')[1] == 'Angeles':
                            citay = 'LA ' + teams[i].split(' ')[2]
                        else:
                            citay = teams[i].split(' ')[0] + ' ' + teams[i].split(' ')[1]
                    else:
                        citay = teams[i].split(' ')[0]
                    p_values = dirt_soup[0].find('td', string=citay).parent
                    tic = 0
                    for tr in p_values.find_all('td'):
                        if tr.find('td', text='Opponents'):
                            break
                        if tic in vals:
                            to_pd.append(tr.string.replace(',', ''))
                            tic += 1
                        else:
                            tic += 1
                dictionario = dict(zip(heads, to_pd))
                df2 = pd.DataFrame.from_dict(dictionario, orient='index').transpose()
                plyr_pos = plyr_pos.append(df2, ignore_index=True)
            except AttributeError as e:
                print(e)
        print(plyr_pos)
    plyr_pos.to_csv('C:\\Users\\...\\PlayersStats.csv')


def joined_data():
    files = ['O', 'D']
    df = pd.DataFrame.from_csv('C:\\Users\\...\\PlayersStats.csv')
    for f in files:
        df2 = pd.DataFrame.from_csv('C:\\Users\\cmazz\\..\\{}_Data.csv'.format(f))
        oldcols = list(df2)
        oldcols = [i if i == 'Year' or i == 'Team' else i + '_' + f for i in oldcols]
        df2.columns = oldcols
        df = pd.merge(df, df2, on=['Year', 'Team'], how='outer')
    df3 = pd.DataFrame.from_csv('C:\\Users\\...\Games_Hist_Data.csv')
    listo = ['Team Home', 'Team Away']
    df3['Date'] = pd.to_datetime(df3['Date'])
    df3['Year'] = df3['Date'].dt.year
    for rr in listo:
        df3['Team'] = df3[rr]
        df3 = pd.merge(df3, df, on=['Year', 'Team'], how='outer')
    df3.groupby(list(df3))
    for r in range(len(df3)):
        if df3['Final Home'][r] > df3['Final Away'][r]:
            df3.loc[r, 'Victory'] = 'H'
        else:
            df3.loc[r, 'Victory'] = 'A'
    df3.to_csv('C:\\Users\\...\\ML_Ready_Data\\DataConsolidation.csv')
    # joined_data()


def rolling_perf():
    df = pd.read_csv('C:\\Users\\...\\DataConsolidation.csv', index_col=0)
    rolling_haiting = ['Final Home', 'Final Away', '1H', '2H', '3H', '4H', '1A', '2A', '3A', '4A',
                       'Home Team First downs', 'Home Team Passing', 'Home Team Total Net Yards',
                       'Home Team Rushing Plays', 'Home Team Net Yards Passing', 'Home Team Average Gain',
                       'Home Team Rushing', 'Home Team Penalty', 'Home Team Net Yards Rushing',
                       'Home Team Total Plays', 'Away Team First downs', 'Away Team Passing',
                       'Away Team Total Net Yards', 'Away Team Rushing Plays', 'Away Team Net Yards Passing',
                       'Away Team Rushing', 'Away Team Penalty', 'Away Team Net Yards Rushing', 'Away Team Total Plays']

    unique = df['Team Home'].unique()
    inplace = ['H', 'A']
    for i in range(len(unique)):
        for inp in inplace:
            if inp == 'H':
                located = 'Home'
                character = 'H'
            else:
                located = 'Away'
                character = 'A'
            clean_df = df[df['Team {}'.format(located)].isin([unique[i]])]
            clean_df = clean_df.sort_values('Date')
            H_A = [rr for rr in rolling_haiting  if character in rr and
                   rr != 'Home Team Average Gain' and rr != 'Away Team Average Gain']
            new_H_A = [rr.replace('{}'.format(located), '').strip() if character in rr and len(rr) > 2
                        else rr.replace(character, '') for rr in H_A]

            new_H_A.extend(['Date', 'Team'])
            H_A.extend(['Date', 'Team {}'.format(located)])
            df2 = clean_df[H_A]
            d = dict(zip(H_A, new_H_A))
            df2.rename(columns=d, inplace=True)
            if inp == 'H':
                fin = df2
            else:
                fin = fin.append(df2, ignore_index=True)
                fin = fin.sort_values('Date')
                fin = fin.reset_index(drop=True)
                for col in fin:
                    if col != 'Date' and col != 'Team':
                        fin['Rolling Mean {}'.format(col)] = fin[col].rolling(15).mean()
        # print(fin)
        # df_tot = fin
        if i == 0:
            to_rolling = fin
        else:
            to_rolling = to_rolling.append(fin, ignore_index=True)

    to_rolling = to_rolling.drop(['Final', '1', '2', '3', '4', 'Team First downs',
                     'Team Passing', 'Team Total Net Yards', 'Team Rushing Plays',
                     'Team Net Yards Passing', 'Team Rushing', 'Team Penalty',
                     'Team Net Yards Rushing', 'Team Total Plays'], 1)

    df = pd.read_csv('C:\\Users\\...DataConsolidation.csv', index_col=0)
    df.drop(rolling_haiting, 1)
    to_rolling.to_csv('C:\\Users\\...\\Rolling_Stats.csv')
