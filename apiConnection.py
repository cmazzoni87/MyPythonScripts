def passparam(const_api ='YOUR QUANDL API KEY', cty ='NY', sprd_by='Monthly', dt_start='1975-02-11', transform=False):
    diffCals = ''
    if transform == True: diffCals = '&transform=rdiff'
    const_web = 'https://www.quandl.com/api/v3/datasets/FMAC/HPI_' + cty + '.json?api_key='\
                + const_api + diffCals +'&collapse=' + sprd_by + '&start_date=' + dt_start
    return const_web
