import h5py
import tables
import os
import numpy
import hdf5_getters as h5g

def printObject(name):
    print('%s' % name)
    return

def is_array(object):
    return isinstance(object, numpy.ndarray)

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
     'similar_artists',
     'artist_terms',
     'artist_terms_freq',
     'artist_terms_weight',
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

def dummy_function(word):
    print(word)

def main():
    path = '/Users/maxenchung/Desktop/untitled/MillionSongSubset/data/'
    directories1 = os.listdir(path)
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
                        print(build_h5_dictionary(h5_file, song_index))


if __name__ == '__main__':
    main()



