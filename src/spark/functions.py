import numpy as np

def transform_to_timestamptz(t):
    """
    Transform GDELT mention datetime to timestamp format
    (YYYY-MM-DD HH:MM:SS)  for TimescaleDB.
    """
    return t[:4]+'-'+t[4:6]+'-'+t[6:8]+' '+t[8:10]+':'+t[10:12]+':'+t[12:14]


def transform_to_timestamptz_daily(t):
    """
    Transform GDELT mention datetime to timestamp format
    (YYYY-MM-DD HH:MM:SS)  for TimescaleDB. Get rid of all
    HH:MM:SS info so that rows can be easily group by timestamp
    to obtain daily averages
    """
    return t[:4]+'-'+t[4:6]+'-'+t[6:8]+' 00:00:00'

def get_quantile(data):
    """
    Return the 0, 0.25, 0.5, 0.75, 1 quantiles of data
    """
    arr = np.array(data)
    q = np.array([0, 0.25, 0.5, 0.75, 1])
    return np.quantile(arr, q).tolist()



def hist_data(data):
    """
    Return number of entry in each bin for a histogram
    of range (-10, 10) with 10 bins. Bin 0 and 11 are
    under/overflow bins
    """
    minVal=-10
    maxVal=10
    nBins=10
    bins = [0]*(nBins+2)
    step = (maxVal - minVal) / float(nBins)
    for d in data:
        if d<minVal:
            bins[0] += 1
        elif d>maxVal:
            bins[nBins+1] += 1
        else:
            for b in range(1, nBins+1):
                if d < minVal+float(b)*step:
                    bins[b] += 1
                    break

    return bins

def is_number(s):
    """
    Check if string can be converted to numneric
    """
    try:
        float(s)
        return True
    except ValueError:
        return False

def is_not_empty(l):
    """
    Check empty element in list
    """
    for e in l:
        if e == '':
            return False
    return True

def has_no_numbers(inputString):
    """
    Check if input string has any numeric character
    """
    return not(any(char.isdigit() for char in inputString))

def read_tax_file(tax_file):
    """
    Read list of taxonomy from file
    """
    tax_list = []
    with open(tax_file) as f:
        for row in f:
            data = row.rstrip().split(',')
            tax_list.append(data[1])
    return tax_list

def read_theme_file(theme_file):
    """
    Read list of themes from file
    """
    theme_list = []
    with open(theme_file) as f:
        for row in f:
            data = row.rstrip().split(',')
            theme_list.append(data[1])
    return theme_list

def read_src_file(src_file):
    """
    Read list of news sources from file
    """
    src_list = []
    with open(src_file) as f:
        for row in f:
            data = row.rstrip().split(',')
            src_list.append(data[1])
    return src_list


def clean_comma(inputString):
    """
    Return substring before a comma
    """
    sep = ','
    return inputString.split(sep, 1)[0]

def clean_comma_list(inputList):
    """
    Map clean_comma() to a list of strings
    """
    return list(map(clean_comma, inputList))

def clean_taxonomy(list_of_themes, rdd_list_of_tax):
    """
    Given a list of themes and a list of taxonomy words,
    remove taxonomy words and commas from each theme
    """
    list_of_tax = rdd_list_of_tax.value
    new_list_of_themes = []
    for theme in list_of_themes:
        new_theme = ''
        subtheme_with_comma = theme.split('_')
        subtheme = clean_comma_list(subtheme_with_comma)

        #If only 1 word in theme, must not be taxonomy
        if len(subtheme) > 1:
            for i in range(len(subtheme)):
                if i<2:
                    if not (subtheme[i] in list_of_tax) and has_no_numbers(subtheme[i]):
                        new_theme+=subtheme[i]+'_'
                else: 
                    new_theme+=subtheme[i]+'_'
        else:
            new_theme+=subtheme[0]+'_'

        new_list_of_themes.append(new_theme[:-1])

    return new_list_of_themes

def pick_first_two(l):
    """
    Return first two elements of a list
    """
    return l[:2]

def get_len(l):
    """
    Return length of a list
    """
    return len(l)
        

