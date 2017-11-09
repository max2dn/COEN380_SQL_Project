
def build_h5_dictionary(h5_file, song_id):
    fields = ['artist_familiarity',
     'artist_hotttnesss',
     'artist_id',
     'artist_mbid',
     'artist_playmeid',
     'artist_7digitalid',
     'artist_latitude',
     'artist_longitude',
     'artist_location',
     'artist_name',
     'release',
     'release_7digitalid',
     'song_id',
     'song_hotttnesss',
     'title',
     'track_7digitalid',
     'similar_artists', #useless
     'artist_terms', #useless
     'artist_terms_freq', #useless
     'artist_terms_weight', #useless
     'analysis_sample_rate',
     'audio_md5',
     'danceability',
     'duration',
     'end_of_fade_in',
     'energy',
     'key',
     'key_confidence',
     'loudness',
     'mode',
     'mode_confidence',
     'start_of_fade_out',
     'tempo',
     'time_signature',
     'time_signature_confidence',
     'track_id',
     'segments_start',
     'segments_confidence',
     'segments_pitches',
     'segments_timbre',
     'segments_loudness_max',
     'segments_loudness_max_time',
     'segments_loudness_start',
     'sections_start',
     'sections_confidence',
     'beats_start',
     'beats_confidence',
     'bars_start',
     'bars_confidence',
     'tatums_start',
     'tatums_confidence',
     'artist_mbtags',
     'artist_mbtags_count',
     'year']

    song_dict = {}
    for field in fields:
        function = 'h5g.get_' + field + '(h5_file, song_id)'
        value = eval(function)
        if is_array(value):
            continue
        song_dict[field] = value

    return song_dict

import re
import csv
import collections

def clean_h5_dictionary(input_dict):

    nullcount = 0
    l = list()
    for key in input_dict:
        if (input_dict[key] is None) or (input_dict[key] is ' '):
            nullcount=+1

    if nullcount >= 4:
        return l

    if not (isinstance(input_dict['artist_familiarity'],float)):
        input_dict['artist_familiarity'] = float(input_dict['artist_familiarity'])

    if not (isinstance(input_dict['artist_hotttnesss'],float)):
        input_dict['artist_hotttnesss'] = float(input_dict['artist_hotttnesss'])

    if ( (input_dict['artist_id'] is None) or (input_dict['artist_id'] is ' ') ):
        return l
    elif not (isinstance(input_dict['artist_id'], basestring)):
        return l
    #missing regex

    mbid = r"\w{8}-\w{4}-\w{4}-\w{4}-\w{12}"
    if ( (input_dict['artist_mbid'] is None) or (input_dict['artist_mbid'] is ' ') ):
        return l
    elif not (isinstance(input_dict['artist_mbid'], basestring)):
        return l
    elif not (re.match(mbid, input_dict['artist_mbid'])):
        return l

    playmeid = r"\d{4}"
    if ( (input_dict['artist_playmeid'] is None) or (input_dict['artist_playmeid'] is ' ') ):
        return l
    elif not (isinstance(input_dict['artist_playmeid'],int)):
        input_dict['artist_playmeid'] = int(input_dict['artist_playmeid'])
    elif not (re.match(playmeid, input_dict['artist_playmeid'])):
        return l

    #missing regex
    if not (isinstance(input_dict['artist_7digitalid'],int)):
        return l

    if not (isinstance(input_dict['artist_latitude'],float)):
        return l

    if not (isinstance(input_dict['artist_longitude'],float)):
        return l

    if not (isinstance(input_dict['artist_location'], basestring)):
        return l

    if not (isinstance(input_dict['artist_name'], basestring)):
        return l

    if not (isinstance(input_dict['release'], basestring)):
        return l

    if ( (input_dict['release_7digitalid'] is None) or (input_dict['release_7digitalid'] is ' ') ):
        return l
    elif not (isinstance(input_dict['release_7digitalid'], int)):
        return l

    if ( (input_dict['song_id'] is None) or (input_dict['song_id'] is ' ') ):
        return l
    elif not (isinstance(input_dict['song_id'], basestring)):
        return l

    if not (isinstance(input_dict['song_hotttnesss'],float)):
        return l

    if not (isinstance(input_dict['title'], basestring)):
        return l

    if ( (input_dict['track_7digitalid'] is None) or (input_dict['track_7digitalid'] is ' ') ):
        return l
    elif not (isinstance(input_dict['track_7digitalid'], int)):
        return l

    #missing range ?
    if not (isinstance(input_dict['analysis_sample_rate'],float)):
        return l

    audio = r"\w{32}"
    if not (isinstance(input_dict['audio_md5'], basestring)):
        return l
    elif not (re.match(audio, input_dict['audio_md5'])):
        return l

    if not (isinstance(input_dict['danceability'],float)):
        return l

    if not (isinstance(input_dict['duration'],float)):
        input_dict['duration'] = float(input_dict['duration'])
    elif (input_dict['duration']<60 or input_dict['duration']>900):
        return l

    if not (isinstance(input_dict['end_of_fade_in'],float)):
        input_dict['end_of_fade_in'] = float(input_dict['end_of_fade_in'])
    elif (input_dict['end_of_fade_in']>input_dict['duration']):
        return l

    if not (isinstance(input_dict['energy'],float)):
        return l

    if not (isinstance(input_dict['key'],int)):
        return l

    if not (isinstance(input_dict['key_confidence'],float)):
        return l

    if not (isinstance(input_dict['loudness'],float)):
        return l

    m = r"\d{1}"
    if not (isinstance(input_dict['mode'],int)):
        return l
    elif not (re.match(m, input_dict['mode'])):
        return l

    if not (isinstance(input_dict['mode_confidence'],float)):
        return l

    if not (isinstance(input_dict['release'], basestring)):
        return l

    if not (isinstance(input_dict['start_of_fade_out'],float)):
        input_dict['duration'] = float(input_dict['duration'])
    elif (input_dict['start_of_fade_out']<input_dict['duration']):
        return l

    if not (isinstance(input_dict['tempo'],float)):
        return l

    if not (isinstance(input_dict['time_signature'],int)):
        return l

    if not (isinstance(input_dict['time_signature_confidence'],float)):
        return l

    if ( (input_dict['track_id'] is None) or (input_dict['track_id'] is ' ') ):
        return l
    elif not (isinstance(input_dict['track_id'], basestring)):
        return l

    ayear = r"\d{4}"
    if not (isinstance(input_dict['year'],int)):
        return l
    elif (input_dict['year'] != 0 and not re.match(ayear, input_dict['year'])):
        return l

    #The next two lines can be used in place of the for loop if the for loop takes too long
    #od=collections.OrderedDict(sorted(input_dict.items()))
    #l = od.values()

    #A list of values of the song dictionary that is entered to the list based on sorted keys
    for key in sorted(input_dict.iterkeys()):
        l.append(input_dict[key])

    return l

def main():
    path = '/Users/maxenchung/Desktop/untitled/MillionSongSubset/data/'
    directories1 = os.listdir(path)
    writeList=list()
    for directory1 in directories1 : #A
        directories2 = os.listdir(path + directory1)
        for directory2 in directories2: #A/A
            directories3 = os.listdir(path + directory1 + '/' + directory2)
            for directory3 in directories3: #A/A/A
                file_path = path + directory1 + '/' + directory2 + '/' + directory3 + '/'
                files = os.listdir(file_path)
                for filename in files:
                    h5_file = h5g.open_h5_file_read(file_path + filename)
                    num_songs = h5g.get_num_songs(h5_file)
                    print(file_path + filename)
                    for song_index in range(0,num_songs):
                        my_dict = build_h5_dictionary(h5_file, song_index)
                        newList=clean_h5_dictionary(my_dict)

                        #See if newList is empty, if it is not then add it to writelist to be put into the csv file
                        if not newList:
                            continue
                        else:
                            writeList.append(newList)

    #Get the keys from my_dict and then sort them and store the sorted keys in k
    k=my_dict.keys()
    k=sorted(k)

    #First write the keys to the output.csv file
    with open('output.csv', 'wb') as csvfile:
        mywriter = csv.writer(csvfile)
        mywriter.writerow(k)

    #Now write the different songs values to the output.csv file
    with open('output.csv', 'a') as csvfile:
        mywriter = csv.writer(csvfile)
        mywriter.writerows(writeList)








