import psycopg2
import psycopg2.extras
import csv
import re
import json
import math
import datetime

import pandas as pd
import numpy as np
import scipy.stats as stats
import statsmodels.api as sm
import matplotlib.pyplot as plt
# import mdp
import pandas.io.sql as pd_sql
import sys

np.set_printoptions(linewidth = 400)
pd.set_option('line_width', 400)
pd.set_option('max_rows', None)
pd.set_option('max_columns', None)


def read_users_db_direct(db_string, date = '2013-09-09', guest = False):
  # heroku pg:credentials DATABASE --app tide-research
  dbconn = psycopg2.connect(db_string)
  cursor = dbconn.cursor()
  if guest == False:
    cursor.execute("""
    SELECT *
    FROM users
    WHERE updated_at >= date '%s' and guest = False
    ORDER BY id;
    """ %( date ))
  else:
    cursor.execute("""
    SELECT *
    FROM users
    WHERE updated_at >= date '%s' and guest = True
    ORDER BY id;
    """ %( date ))

  pieces_flat = []

  users_columns = ['id', 'email', 'created_at', 'updated_at', 'password_digest', 'admin', 'guest', 'name', 'display_name', 'description', 'city', 'state', 'country', 'timezone', 'locale', 'image', 'gender', 'date_of_birth', 'handedness', \
   'orientation', 'education', 'referred_by', 'stats', 'ios_device_token', 'android_device_token', 'is_dob_by_age']

  for row in cursor.fetchall():
    user_db = pd.DataFrame([row], columns = users_columns)
    pieces_flat.append(user_db)

  cursor.close()
  dbconn.close()
  assmt_ids = pd.concat(pieces_flat, ignore_index = True)

  return assmt_ids

def read_db_direct(db_string, ids_to_find, db_name, db_field, db_name_columns):
  # heroku pg:credentials DATABASE --app tide-research
  dbconn = psycopg2.connect(db_string)
  cursor = dbconn.cursor()
  cursor.execute('SELECT * FROM %s where %s = ANY (%s) ORDER BY id;' % (db_name, db_field, "%s"), (ids_to_find,))

  pieces = []
  pieces_flat = []

  for row in cursor.fetchall():
    user_db = pd.DataFrame([row], columns = db_name_columns)
    pieces_flat.append(user_db)

  cursor.close()
  dbconn.close()

  db_flat = pd.concat(pieces_flat, ignore_index = True)

  return db_flat

def read_db_hstore_direct(db_string, ids_to_find, db_name, db_field, db_name_columns):
  dbconn = psycopg2.connect(db_string)
  psycopg2.extras.register_hstore(dbconn)
  cursor = dbconn.cursor()
  cursor.execute('SELECT * FROM %s where %s = ANY (%s) ORDER BY id;' % (db_name, db_field, "%s"), (ids_to_find,))

  pieces = []
  pieces_flat = []

  for row in cursor.fetchall():
    user_db = pd.DataFrame([row], columns=db_name_columns)
    pieces_flat.append(user_db)

  cursor.close()
  dbconn.close()

  db_flat = pd.concat(pieces_flat, ignore_index=True)

  return db_flat


####
def read_users_db(dbname_read, series_ids):
  dbname_read2 = 'dbname=%s' % dbname_read
  #dbconn = psycopg2.connect(database = "heroku_clicked_042413")
  dbconn = psycopg2.connect(dbname_read2)
  cursor = dbconn.cursor()
  cursor.execute("""
  SELECT *
  FROM users
  WHERE guest = FALSE
  ORDER BY id;
  """)

  assmt_pieces = []

  pieces_flat = []
  series_ids_lower = [x.lower() for x in series_ids]

  for row in cursor.fetchall():
    if row[1].lower() in series_ids_lower:
      print "1", row
      assmt_pieces.append(row[0])

      user_db = pd.DataFrame([row])
      pieces_flat.append(user_db)

    try:
      if row[6].lower() in series_ids_lower:
        print "6", row
        assmt_pieces.append(row[6])

        user_db = pd.DataFrame([row])
        pieces_flat.append(user_db)
    except:
      pass

  cursor.close()
  dbconn.close()
  assmt_ids = pd.concat(pieces_flat, ignore_index=True)

  print "\n\n", assmt_pieces, "\n\n"
  print "\n\n", assmt_ids, "\n\n"

  return assmt_ids

def read_users_db_date(dbname_read, date):
  dbname_read2 = 'dbname=%s' % dbname_read
  #dbconn = psycopg2.connect(database = "heroku_clicked_042413")
  dbconn = psycopg2.connect(dbname_read2)
  cursor = dbconn.cursor()
  cursor.execute("""
  SELECT *
  FROM users
  WHERE updated_at >= date '%s'
  ORDER BY id;
  """ %( date ))

  assmt_pieces = []

  pieces_flat = []

  for row in cursor.fetchall():
    assmt_pieces.append(row[0])
    user_db = pd.DataFrame([row])
    pieces_flat.append(user_db)

  cursor.close()
  dbconn.close()
  assmt_ids = pd.concat(pieces_flat, ignore_index=True)

  print "\n\n", assmt_pieces, "\n\n"
  print "\n\n", assmt_ids, "\n\n"

  return assmt_ids

def read_users_db_date_all(dbname_read, date, guest = False):
  dbname_read2 = 'dbname=%s' % dbname_read
  #dbconn = psycopg2.connect(database = "heroku_clicked_042413")
  dbconn = psycopg2.connect(dbname_read2)
  cursor = dbconn.cursor()

  if guest == False:
    cursor.execute("""
    SELECT *
    FROM users
    WHERE updated_at >= date '%s' and guest = False
    ORDER BY id;
    """ %( date ))
  else:
    cursor.execute("""
    SELECT *
    FROM users
    WHERE updated_at >= date '%s' and guest = True
    ORDER BY id;
    """ %( date ))

  pieces_flat = []

  users_columns = ['id', 'email', 'created_at', 'updated_at', 'password_digest', 'admin', 'guest', 'name', 'display_name', 'description', 'city', 'state', 'country', 'timezone', 'locale', 'image', 'gender', 'date_of_birth', 'handedness', \
   'orientation', 'education', 'referred_by', 'stats', 'ios_device_token', 'android_device_token', 'is_dob_by_age']

  for row in cursor.fetchall():
    user_db = pd.DataFrame([row], columns = users_columns)
    pieces_flat.append(user_db)


  cursor.close()
  dbconn.close()
  assmt_ids = pd.concat(pieces_flat, ignore_index=True)

  print "\n\n", assmt_ids, "\n\n"

  return assmt_ids

def read_db(dbname_read, ids_to_find, db_name, db_field, db_name_columns):
  dbname_read2 = 'dbname=%s' % dbname_read
  dbconn = psycopg2.connect(dbname_read2)
  cursor = dbconn.cursor()
  cursor.execute('SELECT * FROM %s where %s = ANY (%s) ORDER BY id;' % (db_name, db_field, "%s"), (ids_to_find,))

  pieces = []
  pieces_flat = []

  for row in cursor.fetchall():
    user_db = pd.DataFrame([row], columns=db_name_columns)
    pieces_flat.append(user_db)

  cursor.close()
  dbconn.close()

  db_flat = pd.concat(pieces_flat, ignore_index=True)

  return db_flat

def read_db_hstore(dbname_read, ids_to_find, db_name, db_field, db_name_columns):
  dbname_read2 = 'dbname=%s' % dbname_read
  dbconn = psycopg2.connect(dbname_read2)
  psycopg2.extras.register_hstore(dbconn)
  cursor = dbconn.cursor()
  cursor.execute('SELECT * FROM %s where %s = ANY (%s) ORDER BY id;' % (db_name, db_field, "%s"), (ids_to_find,))

  pieces = []
  pieces_flat = []

  for row in cursor.fetchall():
    user_db = pd.DataFrame([row], columns=db_name_columns)
    pieces_flat.append(user_db)

  cursor.close()
  dbconn.close()

  db_flat = pd.concat(pieces_flat, ignore_index=True)

  return db_flat
####


def parse_scores_db(_results):
  results = pd.DataFrame(_results, columns=['id', 'game_id', 'score hstore', 'calculations', 'user_id', 'time_played', 'type']).rename(columns={'id': 'results_id'})
  bgpdall_personality = []
  bgpdall = []

  for name, group in results.groupby('game_id'):
    _personality_resut = 0
    bg = {}
    for k, v in  group.iterrows():
      try:
        adj_by = float(v['score hstore']['adjust_by'])
      except:
        pass

      aggresult = json.loads(v['calculations'])
      try:
        for i in aggresult['dimension_values']:
          bg[i] = (aggresult['dimension_values'][i] / 10.0) - adj_by
          _personality_resut = 1
      except:
        pass
      try:
        speed_score = v['score hstore']['speed_score']
        sz_description_id = v['score hstore']['description_id']
        bg['snoozer_speed_score'] = speed_score
        bg['snoozer_description_id'] = sz_description_id
      except:
        pass

    bgpd = pd.DataFrame([bg])

    if _personality_resut == 1:
      bgpd['user_id'] = group['user_id'].iloc[0]
      bgpdall_personality.append(bgpd)
    else:
      bgpd['game_id'] = name
      bgpdall.append(bgpd)
  big5_holland6_df = pd.concat(bgpdall_personality, ignore_index=True)
  other_games_df = pd.concat(bgpdall, ignore_index=True)
  return big5_holland6_df, other_games_df

def parse_results_columns(pd_df):
  _results_db = pd_df.drop(['created_at', 'updated_at', 'intermediate_results'], axis=1)

  pieces = []
  pieces_agg_row = []

  for user_name, user_group in _results_db.groupby('game_id'):
    for row in user_group['event_log']:
      try:
        column7 = json.loads(row)
        for key, resultsrow in enumerate(column7):
          frame = pd.DataFrame([resultsrow])
          frame['game_id'] = user_name
          frame['assessment_id'] = user_name
          pieces.append(frame)
      except:
          pass

      for row_agg in user_group['aggregate_results']:
        try:
          aggresult = json.loads(row_agg)
          print "json", aggresult, "\n\n"

          for keyagg, agg_row in enumerate(aggresult):
            frame3 = pd.DataFrame(aggresult[agg_row])
            frame3['game_id'] = user_name

            pieces_agg_row.append(frame3)
        except:
            pass

  event_log_db = pd.concat(pieces, ignore_index=True)
  agg_results_db = pd.concat(pieces_agg_row)

  bgpdall = []
  for i, v in agg_results_db.groupby('game_id'):
    bg = {}
    for k, val in v['big5'].iteritems():
      try:
        if val['average'] != float('NaN'):
          # bg[k] = val['average'] / val['count']
          bg[k] = val['average']
      except:
        pass

    for a, b in v['holland6'].iteritems():
      try:
        if b['average'] != float('NaN'):
          # bg[a] = b['average'] / b['count']
          bg[a] = b['average']
      except:
        pass


    bgpd = pd.DataFrame([bg])
    bgpd['game_id'] = i
    bgpdall.append(bgpd)

  big5_holland6_df = pd.concat(bgpdall, ignore_index=True)

  return event_log_db, agg_results_db, big5_holland6_df

def parse_games_columns(pd_df):
  _games_db = pd_df.drop(['created_at', 'updated_at'], axis=1)

  pieces = []
  new_way = 1

  for user_name, user_group in _games_db.groupby('id'):
    for row in user_group['event_log']:
      column7 = json.loads(row)
      for i in column7.keys():
        df_stages = pd.DataFrame(column7[i])

        df_stages['user_id'] = user_group['user_id'].iloc[0]
        df_stages['game_id'] = user_name
        df_stages['definition_id'] = user_group['definition_id'].iloc[0]
        df_stages['name'] = user_group['name'].iloc[0]
        pieces.append( df_stages )

  event_log_db = pd.concat(pieces, ignore_index=True)

  return event_log_db

def parse_games_columns_old(pd_df):
  _games_db = pd_df.drop(['created_at', 'updated_at'], axis=1)

  pieces = []
  new_way = 1

  for user_name, user_group in _games_db.groupby('id'):
    #frame = pd.io.json.read_json(user_group['event_log'])
    #print frame
    for row in user_group['event_log']:
      try:
        if new_way == 1:
          #column7 = json.loads(row)
            # print column7[0]
          frame = pd.io.json.read_json(row)
          #   # print frame
          #   # print column7, "\n\n"
          #   # print column7.type, "\n\n"

          frame['definition_id'] = user_group['definition_id'].iloc[0]
          frame['user_id'] = user_group['user_id'].iloc[0]
          pieces.append(frame)

        elif new_way == 0:
          # frame = pd.io.json.read_json(row)
          ### OR ###
          column7 = json.loads(row)
          # print column7[0]
          for key, resultsrow in enumerate(column7):
            frame = pd.DataFrame([resultsrow])
            # print frame
            frame['game_id'] = user_name
            frame['definition_id'] = user_group['definition_id'].iloc[0]
            frame['user_id'] = user_group['user_id'].iloc[0]
            pieces.append(frame)
        #     # frame['user_id'] = user_group['user_id']
        #     # frame['calling_ip'] = user_group['calling_ip']
        #     # frame['assessment_id'] = user_name
      except:
          pass
          #print "ERROR"
    # print "here on parsing game columns"

  event_log_db = pd.concat(pieces, ignore_index=True)

  return event_log_db

def score_tipi(mturk_db):
  mturk_db['Extraversion_tipi'] = (mturk_db['survey_tipi_extraverted'] + ( 8 - mturk_db['survey_tipi_reserved'])) / 2
  mturk_db['Agreeableness_tipi'] = (mturk_db['survey_tipi_sympathetic'] + ( 8 - mturk_db['survey_tipi_critical'])) / 2
  mturk_db['Conscientiousness_tipi'] = (mturk_db['survey_tipi_dependable'] + ( 8 - mturk_db['survey_tipi_disorganized'])) / 2
  mturk_db['Emotional_Stability_tipi'] = (mturk_db['survey_tipi_calm'] + ( 8 - mturk_db['survey_tipi_anxious'])) / 2
  mturk_db['Openness_tipi'] = (mturk_db['survey_tipi_open'] + ( 8 - mturk_db['survey_tipi_conventional'])) / 2
  return mturk_db

def read_mturk_data(mt_batch_name, assmt_id_int = 0):
  # working with MTurk data
  mturk_data = pd.read_csv("%s.csv" % mt_batch_name)

  delete_columns = ['HITId', 'HITTypeId', 'Description', 'Title', 'Keywords', 'CreationTime', 'MaxAssignments', 'RequesterAnnotation',
    'AssignmentDurationInSeconds', 'AutoApprovalDelayInSeconds', 'Expiration', 'NumberOfSimilarHITs',
    'LifetimeInSeconds', 'AssignmentId', 'AcceptTime', 'SubmitTime', 'AutoApprovalTime', 'ApprovalTime',
    'RejectionTime', 'RequesterFeedback', 'LifetimeApprovalRate', 'Last30DaysApprovalRate', 'Last7DaysApprovalRate',
    'Approve', 'Reject', 'Reward']

  mturk_data_v2 = mturk_data.drop(delete_columns, axis=1).rename(columns={'Answer.assessment_code': 'assessment_id'})

  # mturk_data_v2 = mturk_data_v2[mturk_data_v2['AssignmentStatus'] == 'Approved']
  mturk_data_v2 = mturk_data_v2[mturk_data_v2['AssignmentStatus'].isin(['Approved', 'Submitted'])]
  #df[df['A'].isin([3, 6])]
  print mturk_data_v2
  print "here - reading mturk data\n\n"

  # assmt_id_int = 0
  if assmt_id_int == 1:
    mturk_data_v2['assessment_id'] = mturk_data_v2['assessment_id'].astype(np.int32)

  mt_demograic = 0
  if mt_demograic == 1:
    gender_to_num = {
      'Male': 1,
      'Female': 0 }

    handed_to_num = {
      'Right': 1,
      'Left': 2,
      'Both': 3 }

    edu_to_num = {
      "middle_school_or_less": 1,
      "some_high_school": 2,
      "high_school": 3,
      "some_college": 4,
      "associate_degree": 5,
      "bachelor_degree": 6,
      "master_degree": 7,
      "professional_degree": 8,
      "doctoral_degree": 9 }

    sexual_to_num = {
      "straight": 1,
      "gay_lesbian": 2,
      "bisexual": 3,
      "unsure_other": 4 }

    mturk_data_v2['gender'] = mturk_data_v2['Answer.gender'].map(gender_to_num)
    mturk_data_v2['education_level'] = mturk_data_v2['Answer.education_level'].map(edu_to_num)
    mturk_data_v2['handedness'] = mturk_data_v2['Answer.handedness'].map(handed_to_num)
    mturk_data_v2['sexual_orientation'] = mturk_data_v2['Answer.sexual_orientation'].map(sexual_to_num)

  return mturk_data_v2

def parse_event_log(_games_db):
  score = 1
  if score == 1:
    # circle data
    circles_data = pd.read_csv("/Users/mirajsanghvi/code/python/mobile_testing_090213/circles_new.csv")
    circles_data.index = circles_data['name_pair']

  big5_buckets = ['extraversion', 'conscientiousness', 'neuroticism', 'openness', 'agreeableness']

  rt_df_pieces = []
  game_group_dict = {}
  pieces = []

  for name, v_grp in _games_db.iterrows():
    game_group_dict = {}
    definition_id = v_grp['definition_id']

    game_group_dict[name] = {
      'game_id_' + str(definition_id): v_grp['id'], 'user_id': v_grp['user_id'], 'game_name_' + str(definition_id): v_grp['name'],
      'calling_ip_' + str(definition_id): v_grp['calling_ip'], 'status_' + str(definition_id): v_grp['status'], 'date_taken': v_grp['date_taken']
    }
    column7 = json.loads(v_grp['event_log'])

    try:
      for i in column7.keys():
        group2 = pd.DataFrame(column7[i])

        all_events_iter = []
        for events_iter in group2['events']:
          _db = pd.DataFrame([events_iter])
          all_events_iter.append(_db)
        event_log_db = pd.concat(all_events_iter, ignore_index=True)
        pieces.append(event_log_db)

        name2 = group2['stage'].iloc[0]
        varname = str(group2['event_type'].iloc[0])

        try:
          is_touch = str(group2['is_touch'].iloc[0])
          is_phone_width = str(group2['is_phone_width'].iloc[0])
          timezone_offset = str(group2['timezone_offset'].iloc[0])
        except:
          is_touch = 'not_known'
          is_phone_width = 'not_known'
          timezone_offset = 'not_known'

        vars()[varname + "_" + str(name2) + "_time"] = (event_log_db['time'].max() - event_log_db['time'].min())/1000.00
        num_moves = group2['events'].count() - 2

        game_group_dict[name].update({
          varname + "_" + str(name2) + '_total_time': vars()[varname + "_" + str(name2) + "_time"],
          varname + "_" + str(name2) + '_num_moves': num_moves,
          '_is_touch_' + str(definition_id): is_touch,
          '_is_phone_width' + str(definition_id): is_phone_width,
          '_timezone_offset' + str(definition_id): timezone_offset
        })

        if varname == 'circles_test':
          # print "CIRCLES!",

          stage_distance = []
          traits = []
          traits_2 = []

          for name3, group3 in event_log_db.iterrows():

            if group3['event'] == 'level_summary':
              self_top = group3['self_coord']['top']
              self_left = group3['self_coord']['left']
              self_radius = group3['self_coord']['size'] / 2.0

              # for circle vars
              for circle_vals in group3['data']:
                top = circle_vals['top']
                left = circle_vals['left']
                radius = circle_vals['width'] / 2.0
                trait1 = circle_vals['trait1']
                trait2 = circle_vals['trait2']
                size = circle_vals['size']
                # percentage = circle_vals['percentage']
                try:
                  self_percentage = circle_vals['percentage']
                except:
                  self_percentage = 0.0

                result_distance = math.sqrt( ( ((left + radius) - (self_left + self_radius)) ** 2) + ( ((top + radius) - (self_top + self_radius)) ** 2) )

                total_radius = radius + self_radius
                if result_distance >= total_radius:
                  result_overlap = 0.0
                  result_overlap_distance = float('nan')
                elif result_distance <= (self_radius - radius):
                  result_overlap = 1.0
                  result_overlap_distance = float('nan')
                else:
                  result_overlap_distance = total_radius - result_distance
                  result_overlap = (total_radius - result_distance) / (2 * radius)

                stage_distance.append(result_distance)
                stage_distance.sort()
                traits.append(trait1)
                traits_2.append(trait2)

                # new standard distance
                num_self_radius_distance = result_distance / self_radius

                # write to db
                game_group_dict[name].update({
                  varname + "_" + str(name2) + "_" + trait1 + '_distance': result_distance,
                  varname + "_" + str(name2) + "_" + trait1 + '_size': size,
                  varname + "_" + str(name2) + "_" + trait1 + '_overlap': result_overlap,
                  varname + "_" + str(name2) + "_" + trait1 + '_overlap_distance': result_overlap_distance,
                  varname + "_" + str(name2) + "_" + trait1 + '_standard_distance': num_self_radius_distance,
                  varname + "_" + str(name2) + "_" + trait1 + '_percentage': self_percentage
                })

          # rank the circles
          for n, i in enumerate(traits):
            result_distance = game_group_dict[name][varname + "_" + str(name2) + "_" + i + '_distance']
            #rank = stage_distance.index(result_distance) + 1
            rank = stage_distance.index(result_distance)
            game_group_dict[name].update({
              varname + "_" + str(name2) + "_" + i + '_rank': rank
            })

            # work with zscores
            circle_vars_to_get_mean_std_z = ['size', 'rank', 'standard_distance']
            for var in circle_vars_to_get_mean_std_z:
              try:
                circ_value = game_group_dict[name][varname + "_" + str(name2) + "_" + i + '_' + var]

                if var == 'overlap':
                  overlap_mean = circles_data.ix[i+'/'+traits_2[n]][var + '_mean'] / 100
                  overlap_sd = circles_data.ix[i + '/' + traits_2[n]][var + '_sd'] / 100
                  zscore = (circ_value - overlap_mean) / overlap_sd
                else:
                  zscore = (circ_value - circles_data.ix[i+'/'+traits_2[n]][var + '_mean']) / (circles_data.ix[i + '/' + traits_2[n]][var + '_sd'])

                vars()['weighted_zscore_' + var] = zscore * (circles_data.ix[i+'/'+traits_2[n]][var +'_weight'])

                # align with TIPI for scoring
                if var == 'size':
                  if i in ['Self-Reflective', 'Disorganized', 'Anxious', 'Adamant', 'Independent']:
                    zscore = ((5 - circ_value) - circles_data.ix[i+'/'+traits_2[n]][var + '_mean']) / (circles_data.ix[i + '/' + traits_2[n]][var + '_sd'])
                  else:
                    zscore = (circ_value - circles_data.ix[i+'/'+traits_2[n]][var + '_mean']) / (circles_data.ix[i + '/' + traits_2[n]][var + '_sd'])

                  maps_to = circles_data.ix[i+'/'+traits_2[n]]['maps_to']
                  game_group_dict[name].update({
                    varname + "_" + str(name2) + "_" + maps_to+"_zscore": zscore
                  })

              except:
                print "ERROR", name, i, traits_2[n], n, var
                #pass

            try:
              weighted_total = 0
              maps_to = circles_data.ix[i+'/'+traits_2[n]]['maps_to']
              for var in circle_vars_to_get_mean_std_z:
                weighted_total += vars()['weighted_zscore_' + var]

              game_group_dict[name].update({
                varname + "_" + str(name2) + "_" + maps_to: weighted_total
              })
            except:
              pass

        elif varname == 'survey':
          for i in event_log_db[event_log_db['data'].notnull()]['data']:
            for qs2k in i:
              game_group_dict[name].update({
                varname + "_" + qs2k['question_id']: qs2k['answer']
              })

        elif varname == 'interest_picker':
          returned_holl6 = 0
          int_picker = {}
          int_picker_lst = []
          interest_pick_holl6 = {}
          int_picker_deselect = {}

          for _deselect in event_log_db[event_log_db['event'] == 'deselected']['value']:
            returned_holl6 += 1
            int_picker_deselect[_deselect] = 1

          for i in event_log_db[event_log_db['symbol_list'].notnull()]['symbol_list']:
            for holl6pick in i:
              int_picker[holl6pick['record_time']] = holl6pick['value']
              int_picker_lst.append(holl6pick['record_time'])

          for i in event_log_db[event_log_db['word_list'].notnull()]['word_list']:
            for holl6pick in i:
              int_picker[holl6pick['record_time']] = holl6pick['value']
              int_picker_lst.append(holl6pick['record_time'])

          # sort order
          int_picker_lst.sort()
          holl6_count = 1
          max_length = len(int_picker_lst)

          _time_picker = (max(int_picker_lst) - min(int_picker_lst)) / 1000.00
          game_group_dict[name].update({
            varname + "_" + str(name2) + "_time_levels": _time_picker
          })

          for sort_list in int_picker_lst:
            game_group_dict[name].update({
              varname + "_" + str(name2) + "_" + int_picker[sort_list]: 1,
              varname + "_" + str(name2) + "_" + int_picker[sort_list] + '_rank': (max_length - holl6_count)
            })
            holl6_count += 1

          game_group_dict[name].update({
            varname + "_" + str(name2) + "_" + 'returned_option': returned_holl6,
            varname + "_" + str(name2) + "_" + 'total_items_maxlength': max_length
          })

        elif varname == 'snoozer':
          sz_sequence_dict = {}
          _correct_time_diff = []
          _wrong_time_diff = []

          for name3, group3 in event_log_db.iterrows():
            try:
              sz_sequence_dict[group3['item_id']].update({ group3['event']: group3['time'], group3['event'] + '_type': group3['type'] })
            except:
              sz_sequence_dict[group3['item_id']] = { group3['event']: group3['time'], group3['event'] + '_type': group3['type'] }

          correctn = 0
          wrongn = 0
          for k in sz_sequence_dict.keys():
            try:
              time_diff = sz_sequence_dict[k]['correct'] - sz_sequence_dict[k]['shown']
              game_group_dict[name].update({ varname + "_" + str(name2) + '_correct_' + str(correctn + 1): time_diff })
              correctn += 1
              _correct_time_diff.append(time_diff)
            except:
              pass
            try:
              time_diff = sz_sequence_dict[k]['incorrect'] - sz_sequence_dict[k]['shown']
              game_group_dict[name].update({ varname + "_" + str(name2) + '_wrong_' + str(wrongn + 1): time_diff })
              wrongn += 1
              _wrong_time_diff.append(time_diff)
            except:
              pass

          try:
            _min_wrong_time_diff = np.min(_wrong_time_diff)
            _max_wrong_time_diff = np.max(_wrong_time_diff)
          except:
            _min_wrong_time_diff = float('Nan')
            _max_wrong_time_diff = float('Nan')


          game_group_dict[name].update({
            varname + "_" + str(name2) + '_correct_count': correctn,
            varname + "_" + str(name2) + '_wrong_count': wrongn,
            varname + "_" + str(name2) + '_min_correct_time': min(_correct_time_diff),
            varname + "_" + str(name2) + '_max_correct_time': max(_correct_time_diff),
            varname + "_" + str(name2) + '_mean_correct_time': np.mean(_correct_time_diff),
            varname + "_" + str(name2) + '_min_wrong_time': _min_wrong_time_diff,
            varname + "_" + str(name2) + '_max_wrong_time': _max_wrong_time_diff,
            varname + "_" + str(name2) + '_mean_wrong_time': np.mean(_wrong_time_diff)
          })

      rt_df = pd.DataFrame(game_group_dict)
      rt_df_pieces.append(rt_df.T)
    except:
      pass

  if rt_df_pieces:
    reaction_time_dfall = pd.concat(rt_df_pieces, ignore_index=True)
    return reaction_time_dfall
  else:
    return


def pull_data_site(date_to_pull = '2013-09-09', output_file_name = 'none', mturk_batch_name = 'none'):

  multiple_mt_trals = 0
  if multiple_mt_trals == 1:
    list_trials = ['Batch_118__batch_results_all3', 'Batch_1204228_batch_results', 'Batch_1205503_batch_results', 'Batch_1213491_batch_results', 'Batch_1217345_batch_results', 'Batch_all3_batch_results']
    mt_data_all = []
    for a in list_trials:
      _mturk_data = read_mturk_data(a)
      mt_data_all.append(_mturk_data)
    mturk_data = pd.concat(mt_data_all, ignore_index=True)
    mturk_data = mturk_data[mturk_data['assessment_id'].notnull()]

  if mturk_batch_name != 'none':
    mturk_data = read_mturk_data(mturk_batch_name)
    mturk_data = mturk_data[mturk_data['assessment_id'].notnull()]
    list_mturk = np.sort(mturk_data['assessment_id']).tolist()
    assessment_ids = read_users_db(heroku_db_name, list_mturk)
    assmt_id_username = assessment_ids[[0,1,7]].rename(columns={0: 'user_id', 1: 'email', 7:'name'})
    mturk_data['assessment_id_lower'] = mturk_data['assessment_id'].str.lower()
    assmt_id_username['_email_lower'] = assmt_id_username['email'].str.lower()
    _mturk_data_e_n_v2 = pd.merge(assmt_id_username, mturk_data, how='inner', left_on='_email_lower', right_on='assessment_id_lower').drop(['_email_lower', 'assessment_id_lower'], axis=1)
    mturk_data_e_n_v2 = _mturk_data_e_n_v2.drop_duplicates(cols='user_id')
    not_in_db_user = mturk_data[mturk_data['assessment_id_lower'].isin(assmt_id_username['_email_lower']) == False]

  db_string = "dbname=dt4gq525grfd4 host=ec2-54-225-234-119.compute-1.amazonaws.com port=6162 user=u84p9nnc2kv4rm password=p16gj4qnb4pkv159ks7flrikk sslmode=require"

  print "reading databases"
  users_db = read_users_db_direct(db_string, date = date_to_pull, guest = False).rename(columns={'id': 'user_id'})
  list_user_ids = list(users_db['user_id'].T)
  # users_db[users_db['password_digest'] == 'external-authorized account']
  # users_db[['user_id', 'gender', 'date_of_birth', 'handedness', 'education', 'city']]

  # get personality db
  personality_columns = ['id', 'profile_description_id', 'user_id', 'game_id', 'big5_score', 'holland6_score', 'big5_dimension', 'holland6_dimension', 'big5_low', 'big5_high', 'created_at', 'updated_at']
  personality_db = read_db_direct(db_string, list_user_ids, 'personalities', 'user_id', personality_columns).rename(columns={'id': 'personality_id'})
  personality_db_v2 = pd.DataFrame(personality_db, columns=['personality_id', 'profile_description_id', 'user_id', 'big5_dimension', 'holland6_dimension', 'big5_low', 'big5_high'])

  # results
  results_columns = ['id', 'game_id', 'event_log', 'intermediate_results', 'created_at', 'updated_at', 'aggregate_results', 'score hstore', 'calculations', 'user_id', 'time_played', 'time_calculated', 'analysis_version', 'type']
  results_db = read_db_hstore_direct(db_string, list_user_ids, 'results', 'user_id', results_columns)

  # games
  games_columns = ['id', 'date_taken', 'definition_id', 'user_id', 'stages', 'stage_completed', 'created_at', 'updated_at', 'status', 'calling_ip', 'event_log', 'last_error', 'name']
  games_db = read_db_direct(db_string, list_user_ids, 'games', 'user_id', games_columns)
  games_db_usr_ip = pd.DataFrame(games_db, columns = ["id", "calling_ip", "date_taken", "status"]).rename(columns={'id': 'game_id'})

  # profile descriptions
  list_profile_description_ids = list(personality_db['profile_description_id'].unique().T)
  list_profile_description_ids.sort()
  profile_descriptions_columns = ['id', 'name', 'description', 'one_liner', 'bullet_description', 'big5_dimension', 'holland6_dimension', 'code', 'logo_url', 'created_at', 'updated_at', 'display_id']
  profile_descriptions_db = read_db_direct(db_string, list_profile_description_ids, 'profile_descriptions', 'id', profile_descriptions_columns).rename(columns={'id': 'profile_description_id'})
  profile_descriptions_db_v2 = pd.DataFrame(profile_descriptions_db, columns=['profile_description_id', 'name'])


  ### Fitbit
  def get_fitbit_data(heroku_db_name, list_user_ids):
    activities_columns = ['id', 'user_id', 'date_recorded', 'type_id', 'name', 'data', 'goals', 'daily_breakdown', 'provider', 'created_at', 'updated_at']
    activities_db = read_db(heroku_db_name, list_user_ids, 'activities', 'user_id', activities_columns).drop(['type_id', 'name', 'daily_breakdown', 'created_at', 'updated_at'], axis=1).rename(columns={'id': 'activities_id'})

    foods_columns = ['id', 'user_id', 'date_recorded', 'data', 'goals', 'details', 'provider', 'created_at', 'updated_at']
    foods_db = read_db(heroku_db_name, list_user_ids, 'foods', 'user_id', foods_columns).drop(['details', 'created_at', 'updated_at'], axis=1).rename(columns={'id': 'foods_id'})

    sleeps_columns = ['id', 'user_id', 'date_recorded', 'data', 'goals', 'sleep_activity', 'provider', 'created_at', 'updated_at']
    sleeps_db = read_db(heroku_db_name, list_user_ids, 'sleeps', 'user_id', sleeps_columns).drop(['created_at', 'updated_at'], axis=1).rename(columns={'id': 'sleeps_id'})

    measurements_columns = ['id', 'user_id', 'date_recorded', 'data', 'goals', 'details', 'provider', 'created_at', 'updated_at']
    measurements_db = read_db(heroku_db_name, list_user_ids, 'measurements', 'user_id', measurements_columns).drop(['details', 'created_at', 'updated_at'], axis=1).rename(columns={'id': 'measurements_id'})

    # merge together
    act_food = pd.merge(activities_db, foods_db, on=['user_id', 'date_recorded', 'provider'], suffixes=['_activity', '_food'] )
    sleep_measure = pd.merge(sleeps_db, measurements_db, on=['user_id', 'date_recorded', 'provider'], suffixes=['_sleep', '_measurement'] )

    act_food_sleep_measure = pd.merge(act_food, sleep_measure, on=['user_id', 'date_recorded', 'provider'])


  print "working with results"
  results_db_personality_v2, results_db_v2 = parse_scores_db(results_db)
  results_db_personality_v2.drop_duplicates(cols='user_id', take_last=True, inplace=True)

  print "working with games (takes long time - has to parse event_log)"
  def_id_in_batch = []
  for name_def, grp_def in games_db.groupby('definition_id'):
    print 'on games: ', name_def
    vars()['games_db_v3_' + str(name_def)] = parse_event_log(grp_def)
    def_id_in_batch.append(name_def)

  print "extra work with games_dbs"
  try:
    # games_db_v3_4 = games_db_v3_4[games_db_v3_4['status_4'] == 'results_ready']
    vars()['games_db_v3_4'] = vars()['games_db_v3_4'][vars()['games_db_v3_4']['status_4'] == 'results_ready']
  except:
    print "--- ERROR: no valid personality games"
  try:
    ## some work with snoozer data
    vars()['games_db_v3_8']['date_taken_hours'] = vars()['games_db_v3_8']['date_taken'].apply(lambda x: x.time().hour)
    vars()['games_db_v3_8']['snoozer_min_correct_times_sum'] = vars()['games_db_v3_8'][['snoozer_0_min_correct_time', 'snoozer_1_min_correct_time', 'snoozer_2_min_correct_time', 'snoozer_3_min_correct_time']].sum(axis = 1)
    vars()['games_db_v3_8']['snoozer_mean_correct_times_sum'] = vars()['games_db_v3_8'][['snoozer_0_mean_correct_time', 'snoozer_1_mean_correct_time', 'snoozer_2_mean_correct_time', 'snoozer_3_mean_correct_time']].sum(axis = 1)
    vars()['games_db_v3_8']['snoozer_correct_count'] = vars()['games_db_v3_8'][['snoozer_0_correct_count', 'snoozer_1_correct_count', 'snoozer_2_correct_count', 'snoozer_3_correct_count']].sum(axis = 1)
    vars()['games_db_v3_8']['snoozer_wrong_count'] = vars()['games_db_v3_8'][['snoozer_0_wrong_count', 'snoozer_1_wrong_count', 'snoozer_2_wrong_count', 'snoozer_3_wrong_count']].sum(axis = 1)
    vars()['games_db_v3_8']['snoozer_total_time'] = vars()['games_db_v3_8'][['snoozer_0_total_time', 'snoozer_1_total_time', 'snoozer_2_total_time', 'snoozer_3_total_time']].sum(axis = 1)
    vars()['games_db_v3_8']['snoozer_simple_correct_count'] = vars()['games_db_v3_8'][['snoozer_0_correct_count', 'snoozer_1_correct_count']].sum(axis = 1)
    vars()['games_db_v3_8']['snoozer_simple_wrong_count'] = vars()['games_db_v3_8'][['snoozer_0_wrong_count', 'snoozer_1_wrong_count']].sum(axis = 1)
    vars()['games_db_v3_8']['snoozer_simple_total_time'] = vars()['games_db_v3_8'][['snoozer_0_total_time', 'snoozer_1_total_time']].sum(axis = 1)
    vars()['games_db_v3_8']['snoozer_complex_correct_count'] = vars()['games_db_v3_8'][['snoozer_2_correct_count', 'snoozer_3_correct_count']].sum(axis = 1)
    vars()['games_db_v3_8']['snoozer_complex_wrong_count'] = vars()['games_db_v3_8'][['snoozer_2_wrong_count', 'snoozer_3_wrong_count']].sum(axis = 1)
    vars()['games_db_v3_8']['snoozer_complex_total_time'] = vars()['games_db_v3_8'][['snoozer_2_total_time', 'snoozer_3_total_time']].sum(axis = 1)
    vars()['games_db_v3_8']['snoozer_mean_correct_times_sum_sec1to3'] = vars()['games_db_v3_8'][['snoozer_0_mean_correct_time', 'snoozer_1_mean_correct_time', 'snoozer_2_mean_correct_time']].sum(axis = 1)
    vars()['games_db_v3_8']['snoozer_mean_correct_times_sum_sec4'] = vars()['games_db_v3_8'][['snoozer_3_mean_correct_time']].sum(axis = 1)
    vars()['games_db_v3_8']['snoozer_mean_correct_times_sum_sec1to2'] = vars()['games_db_v3_8'][['snoozer_0_mean_correct_time', 'snoozer_1_mean_correct_time']].sum(axis = 1)
    vars()['games_db_v3_8']['snoozer_mean_correct_times_sum_sec3to4'] = vars()['games_db_v3_8'][['snoozer_2_mean_correct_time', 'snoozer_3_mean_correct_time']].sum(axis = 1)
    games_db_v3_8_rename = vars()['games_db_v3_8'].rename(columns={'game_id_8': 'game_id'})
    vars()['games_db_v3_8'] = pd.merge(games_db_v3_8_rename, results_db_v2, on='game_id')
  except:
    print "--- ERROR: no valid snoozer games"

  # for multiple games per user - working to make longitudinal
  for i in def_id_in_batch:
    vars()['games_' + str(i)] = []
    try:
      for name2, group2 in vars()['games_db_v3_' + str(i)].groupby("user_id"):
        if i in (6, 9, 8):
          group2.sort(['date_taken'], inplace=True)
          group2['tvalue'] = group2['date_taken']
          group2 = group2.set_index('tvalue')
          group2['delta'] = (group2['date_taken'] - group2['date_taken'].shift()).fillna(0)
          # time diff and num times played
          _time_delta_sum = group2['delta'].sum()
          _time_delta_sum = _time_delta_sum * 2.77778e-13
          _num_of_times = group2.shape[0]
          group2 = group2.drop(['delta'], axis = 1)
          group2 = group2.rename(columns={'date_taken': 'date_taken_' + str(i)})
          new_games_db = pd.DataFrame([group2.reset_index().unstack().drop(['tvalue', 'user_id']).rename(lambda x: '%s_%d' % x)])
          new_games_db['user_id'] = name2
          new_games_db['num_times_played_' + str(i)] = _num_of_times
          new_games_db['time_delta_sum_' + str(i)] = _time_delta_sum
          new_games_db['time_delta_sum_div_num_' + str(i)] = _time_delta_sum / _num_of_times
          vars()['games_' + str(i)].append(new_games_db)
        else:
          _new_games_db = group2.sort(['date_taken'], ascending=False)
          new_games_db = _new_games_db.iloc[:1].rename(columns={'date_taken': 'date_taken_' + str(i)})
          vars()['games_' + str(i)].append(new_games_db)
      vars()['games_db_v4_' + str(i)] = pd.concat(vars()['games_' + str(i)], ignore_index=True)
    except:
      print "--- ERROR: creating user level data"

  def get_day_cohort():
    df_days = {}
    for name2, group2 in games_db_v3_8.groupby("user_id"):
      group2.sort(['date_taken'], inplace=True)
      _date_taken_start = group2['date_taken'].min()
      group2['delta_day'] = group2['date_taken'].apply(lambda x: (x - _date_taken_start).days)
      for k, v in group2['delta_day'].iteritems():
        try:
          df_days[name2].update({'day' + str(v): 1})
        except:
          df_days[name2] = {'day' + str(v): 1}
    df_date = pd.DataFrame(df_days)

  print "merging user data"
  # games_db_v4_8_v2 = games_dba_v4_8[games_dba_v4_8['num_times_played_8'] <= 14].dropna(axis=1, how="all")
  # user_games_4_8 = pd.merge(games_db_v4_4, games_db_v4_8, on='user_id')
  # user_games_4_8_sorted = user_games_4_8.sort_index(axis = 1)

  _orig_gameid = 0
  games_to_use = (4, 8)
  for i in def_id_in_batch:
    if i in games_to_use:
      if _orig_gameid == 0:
        user_games_all = pd.DataFrame(vars()['games_db_v4_' + str(i)])
        _orig_gameid += 1
      else:
        user_games_all = pd.merge(user_games_all, vars()['games_db_v4_' + str(i)], on='user_id')

  merged_person_result = pd.merge(user_games_all, results_db_personality_v2, on='user_id', how = 'left')
  merged_person_result_v2 = pd.merge(merged_person_result, personality_db_v2, on='user_id')
  merged_person_result_v3 = pd.merge(merged_person_result_v2, profile_descriptions_db_v2, on='profile_description_id')

  def map_big5_holl6(df_to_map):
    big5_low_high_map = {
      'agreeableness': 1,
      'conscientiousness': 2,
      'extraversion': 3,
      'neuroticism': 4,
      'openness': 5 }
    df_to_map['big5_low_map'] = df_to_map['big5_low'].map(big5_low_high_map)
    df_to_map['big5_high_map'] = df_to_map['big5_high'].map(big5_low_high_map)

    holland6_dim_map = {
      'artistic': 1,
      'conventional': 2,
      'enterprising': 3,
      'investigative': 4,
      'realistic': 5,
      'social': 6 }
    df_to_map['holland6_dimension_map'] = df_to_map['holland6_dimension'].map(holland6_dim_map)

    big5_dimension_map = {
      'high_agreeableness': 1,
      'low_agreeableness': 2,
      'high_conscientiousness': 3,
      'low_conscientiousness': 4,
      'high_extraversion': 5,
      'low_extraversion': 6,
      'high_neuroticism': 7,
      'low_neuroticism': 8,
      'high_openness': 9,
      'low_openness': 10 }
    df_to_map['big5_dimension_map'] = df_to_map['big5_dimension'].map(big5_dimension_map)

    return df_to_map

  merged_person_result_v4 = map_big5_holl6(merged_person_result_v3)

  work_with_personality_data_only = 0
  if work_with_personality_data_only == 1:
    merged_person_result_pers = pd.merge(games_db_v4_4, results_db_personality_v2, on='user_id', how = 'left')
    merged_person_result_v2_pers = pd.merge(merged_person_result_pers, personality_db_v2, on='user_id')
    merged_person_result_v3_pers = pd.merge(merged_person_result_v2_pers, profile_descriptions_db_v2, on='profile_description_id')
    merged_person_result_v4_pers = map_big5_holl6(merged_person_result_v3_pers)

  merged_person_result_v4_sorted = merged_person_result_v4.sort_index(axis = 1)

  if output_file_name != 'none':
    merged_person_result_v4_sorted.to_csv(output_file_name + '.csv')

  return merged_person_result_v4


def data_db_work(pandas_df):

  def check_baseline_game():
    mobile_test_db = pd.DataFrame(merged_person_result_v4_pers)

    def score_survey_holl6():
      artistic_survey = ['survey_holl6_Artistic1', 'survey_holl6_Artistic2', 'survey_holl6_Artistic3', 'survey_holl6_Artistic4', 'survey_holl6_Artistic5', 'survey_holl6_Artistic6', 'survey_holl6_Artistic7', \
        'survey_holl6_Artistic8', 'survey_holl6_Artistic9', 'survey_holl6_Artistic10']
      social_survey = ['survey_holl6_Social1', 'survey_holl6_Social2', 'survey_holl6_Social3', 'survey_holl6_Social4', 'survey_holl6_Social5', 'survey_holl6_Social6', 'survey_holl6_Social7', \
        'survey_holl6_Social8', 'survey_holl6_Social9', 'survey_holl6_Social10']
      investigative_survey = ['survey_holl6_Investigative1', 'survey_holl6_Investigative2', 'survey_holl6_Investigative3', 'survey_holl6_Investigative4', 'survey_holl6_Investigative5', 'survey_holl6_Investigative6', 'survey_holl6_Investigative7', \
        'survey_holl6_Investigative8', 'survey_holl6_Investigative9', 'survey_holl6_Investigative10']
      conventional_survey = ['survey_holl6_Conventional1', 'survey_holl6_Conventional2', 'survey_holl6_Conventional3', 'survey_holl6_Conventional4', 'survey_holl6_Conventional5', 'survey_holl6_Conventional6', 'survey_holl6_Conventional7', \
        'survey_holl6_Conventional8', 'survey_holl6_Conventional9', 'survey_holl6_Conventional10']
      realistic_survey = ['survey_holl6_Realistic1', 'survey_holl6_Realistic2', 'survey_holl6_Realistic3', 'survey_holl6_Realistic4', 'survey_holl6_Realistic5', 'survey_holl6_Realistic6', 'survey_holl6_Realistic7', \
        'survey_holl6_Realistic8', 'survey_holl6_Realistic9', 'survey_holl6_Realistic10']
      enterprising_survey = ['survey_holl6_Enterprising1', 'survey_holl6_Enterprising2', 'survey_holl6_Enterprising3', 'survey_holl6_Enterprising4', 'survey_holl6_Enterprising5', 'survey_holl6_Enterprising6', 'survey_holl6_Enterprising7', \
        'survey_holl6_Enterprising8', 'survey_holl6_Enterprising9', 'survey_holl6_Enterprising10']
      all_survey_holl6 = artistic_survey + social_survey + investigative_survey + conventional_survey + realistic_survey + enterprising_survey

      social_vars = ['interest_picker_1_gfx-hol6-social2','interest_picker_1_gfx-hol6-social3','interest_picker_1_gfx-hol6-social5','interest_picker_1_gfx-hol6-social6', 'interest_picker_1_Caring', 'interest_picker_1_Helpful']
      enterprising_vars = ['interest_picker_1_gfx-hol6-enterprising2','interest_picker_1_gfx-hol6-enterprising6','interest_picker_1_gfx-hol6-enterprising4','interest_picker_1_gfx-hol6-enterprising10', 'interest_picker_1_Passionate',  'interest_picker_1_Persuasive']
      conventional_vars = ['interest_picker_1_gfx-hol6-conventional5','interest_picker_1_gfx-hol6-conventional6','interest_picker_1_gfx-hol6-conventional7','interest_picker_1_gfx-hol6-conventional9', 'interest_picker_1_Detail-Oriented', 'interest_picker_1_Thorough']
      realistic_vars = ['interest_picker_1_gfx-hol6-realistic2','interest_picker_1_gfx-hol6-realistic6','interest_picker_1_gfx-hol6-realistic8','interest_picker_1_gfx-hol6-realistic9', 'interest_picker_1_Hands-on', 'interest_picker_1_Mechanical']
      artistic_vars = ['interest_picker_1_gfx-hol6-artistic4','interest_picker_1_gfx-hol6-artistic5','interest_picker_1_gfx-hol6-artistic7','interest_picker_1_gfx-hol6-artistic8', 'interest_picker_1_Creative', 'interest_picker_1_Intuitive']
      investigative_vars = ['interest_picker_1_gfx-hol6-investigative6','interest_picker_1_gfx-hol6-investigative9','interest_picker_1_gfx-hol6-investigative10','interest_picker_1_gfx-hol6-investigative5', 'interest_picker_1_Analytical', 'interest_picker_1_Inquisitive']
      all_holland6_vars = artistic_vars + social_vars + investigative_vars + conventional_vars + realistic_vars + enterprising_vars
      other_holland_vars = ['interest_picker_1_time_levels', 'interest_picker_1_total_time', 'interest_picker_1_total_items_maxlength']
      all_holland6_vars_p_other = all_holland6_vars + other_holland_vars

      def add_one(x):
        if x == 1:
          x += 1
          return x
      boost_plus_1 = ['interest_picker_1_gfx-hol6-artistic5', 'interest_picker_1_gfx-hol6-enterprising2', 'interest_picker_1_gfx-hol6-investigative6', 'interest_picker_1_gfx-hol6-realistic6', 'interest_picker_1_gfx-hol6-social5', 'interest_picker_1_gfx-hol6-conventional9']
      for b in boost_plus_1:
        mobile_test_db[b] = mobile_test_db[b].apply(add_one)

      mobile_test_db['interest_picker_1_h6_sumall'] = pd.DataFrame(mobile_test_db, columns = all_holland6_vars, dtype = 'float64').fillna(0).sum(axis = 1)
      mobile_test_db['holland6_survey_all'] = pd.DataFrame(mobile_test_db, columns = all_survey_holl6, dtype = 'float64').fillna(0).sum(axis = 1)

      holland6_vars = ['artistic', 'social', 'investigative', 'conventional', 'realistic', 'enterprising']
      holl6_survey_sum_cols = []
      holl6_survey_sum_cols_pct = []
      holl_6_picker_sum = []
      holl_6_picker_sum_pct = []
      holl_6_picker_sum_zscore = []
      for i in holland6_vars:
        mobile_test_db['survey_sum_' + i] = pd.DataFrame(mobile_test_db, columns = vars()[i + '_survey'], dtype = 'float64').applymap(lambda x: x - 1).fillna(0).sum(axis = 1)
        mobile_test_db['int_p_1_h6_' + i] = pd.DataFrame(mobile_test_db, columns = vars()[i + '_vars'], dtype = 'float64').fillna(0).sum(axis = 1)
        mobile_test_db['survey_sum_pct_' + i] = mobile_test_db['survey_sum_' + i] / mobile_test_db['holland6_survey_all']
        mobile_test_db['int_p_1_h6_pct_' + i] = mobile_test_db['int_p_1_h6_' + i] / mobile_test_db['interest_picker_1_h6_sumall']
        holl_6_picker_sum.append('int_p_1_h6_' + i)

        def add_one_to_zero(x):
          if x == 0:
            x += 1
            return x
          else:
            return x
        mobile_test_db['int_p_1_h6_fixzero_' + i] = mobile_test_db['int_p_1_h6_' + i].apply(add_one_to_zero)
        mobile_test_db['int_p_divall' + i] = (mobile_test_db['int_p_1_h6_pct_' + i] / mobile_test_db['int_p_1_h6_fixzero_' + i])

        _mean = mobile_test_db['survey_sum_' + i].mean()
        _std = mobile_test_db['survey_sum_' + i].std()
        mobile_test_db['survey_sum_zscore_' + i] = mobile_test_db['survey_sum_' + i].apply(lambda x: (((x - _mean) / _std) * .10) + .50)

        holl6_survey_sum_cols.append('survey_sum_' + i)
        holl6_survey_sum_cols_pct.append('survey_sum_pct_' + i)
        holl_6_picker_sum_zscore.append('survey_sum_zscore_' + i)
        holl_6_picker_sum.append('int_p_1_h6_' + i)
        holl_6_picker_sum_pct.append('int_p_1_h6_pct_' + i)


      mobile_test_db['holland6_trait'] = pd.DataFrame(mobile_test_db, columns = holl_6_picker_sum).idxmax(axis=1).str[21:]
      mobile_test_db['holland6_trait_survey'] = pd.DataFrame(mobile_test_db, columns = holl6_survey_sum_cols).idxmax(axis=1).str[11:]
      mobile_test_db['holland6_trait_pctmax'] = pd.DataFrame(mobile_test_db, columns = holl_6_picker_sum_pct).idxmax(axis=1).str[25:]
      mobile_test_db['holland6_trait_zscore'] = pd.DataFrame(mobile_test_db, columns = holl_6_picker_sum_zscore).idxmax(axis=1).str[18:]
      test = (mobile_test_db['holland6_trait_survey'] == mobile_test_db['holland6_trait_pctmax'])
      test_v2 = (mobile_test_db['holland6_trait_zscore'] == mobile_test_db['holland6_trait_pctmax'])
      test_v3 = (mobile_test_db['holland6_trait_survey'] == mobile_test_db['holland6_trait'])
      test_v4 = (mobile_test_db['holland6_trait_zscore'] == mobile_test_db['holland6_trait'])

      for i in holland6_vars:
        for v in vars()[i + '_vars']:
          mobile_test_db[v + '_pct'] = mobile_test_db[v].astype('float64') * mobile_test_db['int_p_1_h6_pct_' + i]
          mobile_test_db[v + '_pct2'] = mobile_test_db[v].astype('float64') / mobile_test_db['interest_picker_1_h6_sumall']


      def _OLS_holland6():
        for n, i in enumerate(holl_6_picker_sum_zscore):
          print "\n\n\n", i
          _df_orig = pd.DataFrame(mobile_test_db, columns = [i] + [holl_6_picker_sum_pct[n]], dtype = 'float64').fillna(0)
          _df = _df_orig[_df_orig[i].notnull()]
          _df2 = pd.DataFrame(_df, columns = [holl_6_picker_sum_pct[n]], dtype = 'float64').fillna(0)
          _df2 = sm.add_constant(_df2, prepend=False)
          results = sm.OLS(_df[i], _df2).fit()
          print results.summary(), "\n"


    def score_survey_big5():
      big5_cols = ['extraversion', 'conscientiousness', 'neuroticism', 'agreeableness', 'openness']
      # 50 question big 5
      big5_pos_extraversion = ['survey_big5_extraversion1', 'survey_big5_extraversion3', 'survey_big5_extraversion5', 'survey_big5_extraversion7', 'survey_big5_extraversion9']
      big5_neg_extraversion = ['survey_big5_extraversion2', 'survey_big5_extraversion4', 'survey_big5_extraversion6', 'survey_big5_extraversion8', 'survey_big5_extraversion10']
      big5_pos_agreeableness = ['survey_big5_agreeableness2', 'survey_big5_agreeableness4', 'survey_big5_agreeableness6', 'survey_big5_agreeableness8', 'survey_big5_agreeableness10', 'survey_big5_agreeableness9']
      big5_neg_agreeableness = ['survey_big5_agreeableness1', 'survey_big5_agreeableness3', 'survey_big5_agreeableness5', 'survey_big5_agreeableness7']
      big5_pos_conscientiousness = ['big5_conscientiousness1', 'survey_big5_conscientiousness3', 'survey_big5_conscientiousness5', 'survey_big5_conscientiousness7', 'survey_big5_conscientiousness9', 'survey_big5_conscientiousness10']
      big5_neg_conscientiousness = ['survey_big5_conscientiousness2', 'survey_big5_conscientiousness4', 'survey_big5_conscientiousness6', 'survey_big5_conscientiousness8']
      big5_pos_neuroticism = ['survey_big5_emotional_stability2', 'survey_big5_emotional_stability4']
      big5_neg_neuroticism = ['big5_emotional_stability1', 'survey_big5_emotional_stability3', 'survey_big5_emotional_stability5', 'survey_big5_emotional_stability7', 'survey_big5_emotional_stability9', 'survey_big5_emotional_stability10', 'survey_big5_emotional_stability6', 'survey_big5_emotional_stability8']
      big5_pos_openness = ['big5_openness1', 'survey_big5_openness3', 'survey_big5_openness5', 'survey_big5_openness7', 'survey_big5_openness9', 'survey_big5_openness10', 'survey_big5_openness8']
      big5_neg_openness = ['survey_big5_openness2', 'survey_big5_openness4', 'survey_big5_openness6']

      big5_score_columns = []
      big5_score_columns_total = []
      big5_score_columns_total_zs = []
      for i in big5_cols:
        mobile_test_db['big5_pos_' + i] = pd.DataFrame(mobile_test_db, columns = vars()['big5_pos_' + i], dtype = 'float64').fillna(0).sum(axis = 1)
        mobile_test_db['big5_neg_' + i] = pd.DataFrame(mobile_test_db, columns = vars()['big5_neg_' + i], dtype = 'float64').fillna(0).apply(lambda x: 6 - x).sum(axis = 1)
        mobile_test_db['big5_total_' + i] = mobile_test_db['big5_pos_' + i] + mobile_test_db['big5_neg_' + i]
        _mean = mobile_test_db['big5_total_' + i].mean()
        _std = mobile_test_db['big5_total_' + i].std()
        mobile_test_db['big5_total_zscore_' + i] = mobile_test_db['big5_total_' + i].apply(lambda x: (x - _mean) / _std)
        big5_score_columns.append('big5_pos_' + i)
        big5_score_columns.append('big5_neg_' + i)
        big5_score_columns.append('big5_total_' + i)
        big5_score_columns_total.append('big5_total_' + i)
        big5_score_columns_total_zs.append('big5_total_zscore_' + i)
      mobile_test_db['big5_trait_survey'] = pd.DataFrame(mobile_test_db, columns = big5_score_columns_total_zs).idxmax(axis=1).str[18:]
      big5_cols = ['extraversion', 'conscientiousness', 'neuroticism', 'agreeableness', 'openness']
      ## circle vars
      circles_1 = ['circles_test_0_Sociable_', 'circles_test_0_Self-Disciplined_', 'circles_test_0_Anxious_', 'circles_test_0_Cooperative_', 'circles_test_0_Curious_']
      circles_3 = ['circles_test_2_Self-Reflective_', 'circles_test_2_Disorganized_', 'circles_test_2_Calm_', 'circles_test_2_Judgmental_', 'circles_test_2_Traditional_']
      # circle_vars = ['rank', 'size', 'standard_distance', 'percentage']
      circle_vars = ['size']
      circle_vars = ['percentage', 'standard_distance', 'size']
      # circle_vars = ['percentage']

      # for c in circles_1:
      _means_and_std = []
      big5_vars_all = []
      for n, i in enumerate(circles_1):
        vars()[big5_cols[n] + '_big5'] = []
        vars()[big5_cols[n] + '_big5_zs'] = []
        for v in circle_vars:
          big5_vars_all.append(i + v)
          big5_vars_all.append(circles_3[n] + v)
          vars()[big5_cols[n] + '_big5'].append(i + v)
          vars()[big5_cols[n] + '_big5'].append(circles_3[n] + v)
          mobile_test_db[i + v + '_mean'] = mobile_test_db[i + v].mean()
          mobile_test_db[i + v + '_std'] = mobile_test_db[i + v].std()
          mobile_test_db[i + v + '_zscore'] = mobile_test_db[i + v].apply(lambda x: (x - mobile_test_db[i + v + '_mean'][0]) / mobile_test_db[i + v + '_std'][0])
          mobile_test_db[circles_3[n] + v + '_mean'] = mobile_test_db[circles_3[n] + v].mean()
          mobile_test_db[circles_3[n] + v + '_std'] = mobile_test_db[circles_3[n] + v].std()
          mobile_test_db[circles_3[n] + v + '_zscore'] = mobile_test_db[circles_3[n] + v].apply(lambda x: (x - mobile_test_db[circles_3[n] + v + '_mean'][0]) / mobile_test_db[circles_3[n] + v + '_std'][0])
          vars()[big5_cols[n] + '_big5_zs'].append(i + v + '_zscore')
          vars()[big5_cols[n] + '_big5_zs'].append(circles_3[n] + v + '_zscore')
          _means_and_std.append(i + v + '_mean')
          _means_and_std.append(i + v + '_std')
          _means_and_std.append(circles_3[n] + v + '_mean')
          _means_and_std.append(circles_3[n] + v + '_std')

    # holl6_survey_sum_cols, holl_6_picker_sum
    holl6_mobile_mobile_test_db = pd.DataFrame(mobile_test_db, columns = holl6_survey_sum_cols + holl_6_picker_sum, dtype = 'float64')
    # big5_vars_all, big5_score_columns
    big5_mobile_mobile_test_db = pd.DataFrame(mobile_test_db, columns = big5_vars_all + big5_score_columns, dtype = 'float64')

    big5_mobile_test_db_mean_std_v2 = pd.DataFrame(mobile_test_db, columns = _means_and_std, dtype = 'float64')

    # OLS
    def _OLS():
      for n, i in enumerate(big5_score_columns_total_zs):
        print "\n\n\n", i
        _df_orig = pd.DataFrame(mobile_test_db, columns = [i] + vars()[big5_cols[n] + '_big5_zs'], dtype = 'float64').fillna(0)
        _df = _df_orig[_df_orig[i].notnull()]
        _df2 = pd.DataFrame(_df, columns = vars()[big5_cols[n] + '_big5_zs'], dtype = 'float64').fillna(0)
        _df2['const']  = 1
        results = sm.OLS(_df[i], _df2).fit()
        print results.summary(), "\n"

    # logistic regression
    def _logistic_regression():
      dummy_ranks = pd.get_dummies(merged_person_result_v5['holland6_trait_survey'], prefix='holl6')
      data = merged_person_result_v5[all_holland6_vars].join(dummy_ranks).fillna(0).astype(np.float64)
      data['intercept'] = 1.0
      for i in holland6_vars:
        print i
        try:
          logit = sm.Logit(data['holl6_survey_sum_' + str(i)], data[vars()[str(i) + '_vars'] + ['intercept']])
          vars()[i + '_result'] = logit.fit()
          print vars()[i + '_result'].summary()
          print '\n'
        except:
          print "pass"

    def k_means(clusters, x_variables, y_variable, y_variable_txt):
      from sklearn.cluster import KMeans, MiniBatchKMeans
      from sklearn.preprocessing import StandardScaler
      df = pd.DataFrame(mobile_test_db, columns = x_variables, dtype = 'float64').fillna(0)
      df_X = StandardScaler().fit_transform(df)
      km2 = KMeans(init='random', n_clusters=clusters)
      km2.fit(df_X)
      km2_labels = pd.DataFrame(km2.labels_, columns = ['kmeans'])
      km_compare = pd.concat([km2_labels['kmeans'], mobile_test_db[y_variable]], axis = 1)
      km_compare_txt = pd.concat([km2_labels['kmeans'], mobile_test_db[y_variable_txt]], axis = 1)
      plt.scatter(km_compare['kmeans'], km_compare[y_variable], alpha=0.02)
      kmeans_crosstab = pd.crosstab(km_compare_txt[y_variable_txt], km_compare_txt['kmeans'], rownames=['actual'], colnames=['preds'])
      print kmeans_crosstab.to_string()

    k_means(10, big5_vars_all, 'big5_dimension_map', 'big5_dimension')
    k_means(6, holl_6_picker_sum, 'holland6_dimension_map', 'holland6_dimension')

    def kohonen_map():
      # dataframe_pca, num_facs, rotation = "none"):
      import pandas.rpy.common as com
      import rpy2.robjects as robjects
      from rpy2.robjects.packages import importr

      r_som = importr('som')
      df_b5_pd = pd.DataFrame(mobile_test_db, columns= big5_vars_all, dtype = 'float64').fillna(0)
      zscore = lambda x: abs(x - x.mean()) / x.std()
      transformed = df_b5_pd.apply(zscore)
      r_dataframe = com.convert_to_r_dataframe(transformed)
      test_som = r_som.som( data = r_dataframe, xdim=1, ydim = 6 )
      test.som <- som( data = mydata, xdim=1, ydim = 6 )
      robjects.r.numeric(r_dataframe[0])

    ## random forest
    def _random_forest(y_variable, x_variables):
      from sklearn.ensemble import RandomForestClassifier
      df = pd.DataFrame(mobile_test_db, columns= x_variables + [y_variable]).fillna(0)
      df['is_train'] = np.random.uniform(0, 1, len(df)) <= .75
      train, test = df[df['is_train']==True], df[df['is_train']==False]
      features = x_variables
      clf = RandomForestClassifier(n_jobs=2)
      y, _ = pd.factorize(train[y_variable])
      clf.fit(train[features], y)
      preds = train[y_variable].unique()[clf.predict(test[features])]
      randforrest = pd.crosstab(test[y_variable], preds, rownames=['actual'], colnames=['preds'])
      print randforrest.to_string()

    _random_forest('big5_dimension', big5_vars_all)
    _random_forest('holland6_dimension', holl_6_picker_sum)

    # to scale:
    def _scale_data():
      from sklearn.preprocessing import StandardScaler
      X = StandardScaler().fit_transform(X)

    ## SVM
    def _SVM():
      df = pd.DataFrame(merged_person_result_v5, columns= all_holland6_vars + ['holland6_trait_survey']).fillna(0)
      df['holland6_trait_survey_fac'], _ = pd.factorize(df['holland6_trait_survey'])
      df_flt = pd.DataFrame(merged_person_result_v5, columns= all_holland6_vars).fillna(0).astype(np.float64)
      Y = df['holland6_trait_survey_fac']
      X = np.array(df_flt)

      from sklearn import svm
      clf = svm.SVC()
      clf.fit(X, Y)
      dec = clf.decision_function(1)
      dec.shape[1]

      lin_clf = svm.LinearSVC()
      lin_clf.fit(X, Y)
      dec = lin_clf.decision_function([X[1]])
      dec.shape[1]

      clf = svm.NuSVC()
      clf.fit(X, Y)

    ## DBSCAN
    def _DBSCAN():
      from sklearn.cluster import DBSCAN
      db_scan = DBSCAN(eps=0.3, min_samples=10).fit(df_flt)
      core_samples = db_scan.core_sample_indices_
      labels = db_scan.labels_
      n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)

    ## kNN
    def _kNN():
      import pandas as pd
      import pylab as pl
      from sklearn.neighbors import KNeighborsClassifier
      # from sklearn.neighbors import MiniBatchKMeans
      df = pd.DataFrame(merged_person_result_v5, columns= all_holland6_vars + ['holland6_trait_survey']).fillna(0)
      test_idx = np.random.uniform(0, 1, len(df)) <= 0.3
      train = df[test_idx==True]
      test = df[test_idx==False]

      features = all_holland6_vars
      results = []
      for n in range(1, 51, 2):
          clf = KNeighborsClassifier(n_neighbors=n)
          clf.fit(train[features], train['holland6_trait_survey'])
          preds = clf.predict(test[features])
          accuracy = np.where(preds==test['holland6_trait_survey'], 1, 0).sum() / float(len(test))
          print "Neighbors: %d, Accuracy: %3f" % (n, accuracy)
          results.append([n, accuracy])

      results = pd.DataFrame(results, columns=["n", "accuracy"])
      # plot to find out how many k to use.
      pl.plot(results.n, results.accuracy)
      pl.title("Accuracy with Increasing K")
      pl.show()

      # figuring out which method works best.=
      results = []
      for w in ['uniform', 'distance', lambda x: np.log(x)]:
          clf = KNeighborsClassifier(25, weights=w)
          w = str(w)
          clf.fit(train[features], train['holland6_trait_survey'])
          preds = clf.predict(test[features])
          accuracy = np.where(preds==test['holland6_trait_survey'], 1, 0).sum() / float(len(test))
          print "Weights: %s, Accuracy: %3f" % (w, accuracy)
          results.append([w, accuracy])

      results = pd.DataFrame(results, columns=["weight_method", "accuracy"])
      print results

    #Bayes
    def _naive_bayes(y_variable, x_variables, multinom = 0, get_zs = 0):
      from sklearn.naive_bayes import GaussianNB
      from sklearn.naive_bayes import MultinomialNB

      df = pd.DataFrame(mobile_test_db, columns= x_variables + [y_variable]).fillna(0)
      df[y_variable + '_fac'], _ = pd.factorize(df[y_variable])
      zscore = lambda x: abs(x - x.mean()) / x.std()
      if get_zs == 1:
        df_flt = pd.DataFrame(mobile_test_db, columns= x_variables).fillna(0).astype(np.float64).apply(zscore)
      else:
        df_flt = pd.DataFrame(mobile_test_db, columns= x_variables).fillna(0).astype(np.float64)

      gnb = GaussianNB()
      y_pred = gnb.fit(df_flt, df[y_variable + '_fac']).predict(df_flt)
      print("Number of mislabeled points : %d" % (df[y_variable + '_fac'] != y_pred).sum())
      print "pct:", (df[y_variable + '_fac'] != y_pred).sum() / float(df_flt.shape[0]), "\n"

      if multinom == 1:
        mnb = MultinomialNB()
        y_pred_mnb = mnb.fit(df_flt, df[y_variable + '_fac']).predict(df_flt)
        print("Number of mislabeled points : %d" % (df[y_variable + '_fac'] != y_pred_mnb).sum())
        print "pct:", (df[y_variable + '_fac'] != y_pred_mnb).sum() / float(df_flt.shape[0]), "\n"

    time_move_vars_intp = ['interest_picker_1_total_items_maxlength', 'interest_picker_1_total_time']
    time_move_vars_circ = ['circles_test_0_num_moves', 'circles_test_0_total_time', 'circles_test_2_num_moves', 'circles_test_2_total_time']
    _naive_bayes( y_variable = 'big5_dimension_map', x_variables = big5_vars_all + holl_6_picker_sum)
    _naive_bayes( y_variable = 'holland6_dimension_map', x_variables = holl_6_picker_sum)

    def check_how_many_big5_close_plot():
      big5_cols = ['extraversion', 'conscientiousness', 'neuroticism', 'agreeableness', 'openness']

      mobile_test_db['big5_mean'] = mobile_test_db[big5_cols].mean(axis=1)
      _big5_abs_vals = []
      _big5_abs_vals_perct = []
      for i in big5_cols:
        mobile_test_db[i + '_diffabs'] = (mobile_test_db[i] - mobile_test_db['big5_mean']).abs()
        _big5_abs_vals.append(i + '_diffabs')
      mobile_test_db['big5_maxabs'] = mobile_test_db[_big5_abs_vals].max(axis=1)
      for i in big5_cols:
        mobile_test_db[i + '_absperc'] = mobile_test_db[i + '_diffabs'] /  mobile_test_db['big5_maxabs']
        _big5_abs_vals_perct.append(i + '_absperc')
      # mobile_test_db[_big5_abs_vals_perct]
      mobile_test_db[_big5_abs_vals_perct].hist(bins = 25)

      holland6_vars = ['artistic', 'social', 'investigative', 'conventional', 'realistic', 'enterprising']
      mobile_test_db['holl6_mean'] = mobile_test_db[holland6_vars].mean(axis=1)
      _holl6_abs_vals = []
      _holl6_abs_vals_perct = []
      for i in holland6_vars:
        mobile_test_db[i + '_diffabs'] = (mobile_test_db[i] - mobile_test_db['holl6_mean']).abs()
        _holl6_abs_vals.append(i + '_diffabs')
      mobile_test_db['holl6_maxabs'] = mobile_test_db[_holl6_abs_vals].max(axis=1)
      for i in holland6_vars:
        mobile_test_db[i + '_absperc'] = mobile_test_db[i + '_diffabs'] /  mobile_test_db['holl6_maxabs']
        _holl6_abs_vals_perct.append(i + '_absperc')
      mobile_test_db[_holl6_abs_vals_perct].hist(bins = 25)

    def plot_mean_correct_times():
      _sec13 = []
      _sec4 = []
      for i in range(0, 6):
        _sec13.append('snoozer_mean_correct_times_sum_sec1to3_' + str(i))
        _sec4.append('snoozer_mean_correct_times_sum_sec4_' + str(i))
      _plot_mean_times = pd.DataFrame( merged_person_result_v4, columns = _sec13 + ['big5_dimension'] )
      _df_plot_all = []
      for k, v in _plot_mean_times.groupby('big5_dimension'):
        v_d = v.dropna(axis = 0, how='any')
        print v_d
        _df = pd.DataFrame(v_d.mean()).T
        _df['big5_dimension'] = k
        _df_plot_all.append(_df)
      _df_plot_all2 = pd.concat(_df_plot_all, ignore_index=True)
      _df_plot_all3 = _df_plot_all2.set_index(_df_plot_all2['big5_dimension']).drop(['big5_dimension'], axis = 1)
      _df_plot_all3.T.plot()



    circle_cols = ['circles_test_4_Creative_distance',  'circles_test_4_Creative_overlap', 'circles_test_4_Creative_overlap_distance',  'circles_test_4_Creative_rank',  'circles_test_4_Creative_size',  'circles_test_4_Creative_standard_distance', \
      'circles_test_4_Detail-Oriented_distance', 'circles_test_4_Detail-Oriented_overlap',  'circles_test_4_Detail-Oriented_overlap_distance', 'circles_test_4_Detail-Oriented_rank', 'circles_test_4_Detail-Oriented_size', 'circles_test_4_Detail-Oriented_standard_distance', \
      'circles_test_4_Inquisitive_distance', 'circles_test_4_Inquisitive_overlap',  'circles_test_4_Inquisitive_overlap_distance', 'circles_test_4_Inquisitive_rank', 'circles_test_4_Inquisitive_size', 'circles_test_4_Inquisitive_standard_distance', \
      'circles_test_4_Hands-on_distance', 'circles_test_4_Hands-on_overlap', 'circles_test_4_Hands-on_overlap_distance',  'circles_test_4_Hands-on_rank',  'circles_test_4_Hands-on_size',  'circles_test_4_Hands-on_standard_distance', \
      'circles_test_4_Persuasive_distance',  'circles_test_4_Persuasive_overlap', 'circles_test_4_Persuasive_overlap_distance',  'circles_test_4_Persuasive_rank',  'circles_test_4_Persuasive_size',  'circles_test_4_Persuasive_standard_distance', \
      'circles_test_4_Helpful_distance', 'circles_test_4_Helpful_overlap',  'circles_test_4_Helpful_overlap_distance', 'circles_test_4_Helpful_rank', 'circles_test_4_Helpful_size', 'circles_test_4_Helpful_standard_distance',  'circles_test_4_artistic', \
      'circles_test_4_artistic_zscore',  'circles_test_4_conventional', 'circles_test_4_conventional_zscore',  'circles_test_4_enterprising', 'circles_test_4_enterprising_zscore',  'circles_test_4_investigative',  'circles_test_4_investigative_zscore', \
      'circles_test_4_num_moves',  'circles_test_4_realistic',  'circles_test_4_realistic_zscore', 'circles_test_4_social', 'circles_test_4_social_zscore', 'circles_test_4_total_time']

    interest_pick_cols = ['interest_picker_0_Analytical',  'interest_picker_0_Caring',  'interest_picker_0_Creative',  'interest_picker_0_Detail-Oriented', 'interest_picker_0_Hands-on',  'interest_picker_0_Helpful', \
      'interest_picker_0_Inquisitive', 'interest_picker_0_Intuitive', 'interest_picker_0_Mechanical',  'interest_picker_0_Passionate',  'interest_picker_0_Persuasive',  'interest_picker_0_Thorough',  \
      "interest_picker_0_gfx-hol6-social1", "interest_picker_0_gfx-hol6-social2", "interest_picker_0_gfx-hol6-social3", "interest_picker_0_gfx-hol6-social5", \
      "interest_picker_0_gfx-hol6-enterprising2", "interest_picker_0_gfx-hol6-enterprising6", "interest_picker_0_gfx-hol6-enterprising7", "interest_picker_0_gfx-hol6-investigative4", \
      "interest_picker_0_gfx-hol6-conventional5", "interest_picker_0_gfx-hol6-conventional6", "interest_picker_0_gfx-hol6-conventional7", "interest_picker_0_gfx-hol6-conventional8", \
      "interest_picker_0_gfx-hol6-realistic2", "interest_picker_0_gfx-hol6-realistic6", "interest_picker_0_gfx-hol6-realistic7", "interest_picker_0_gfx-hol6-enterprising3", \
      "interest_picker_0_gfx-hol6-artistic4", "interest_picker_0_gfx-hol6-artistic5", "interest_picker_0_gfx-hol6-artistic6", "interest_picker_0_gfx-hol6-artistic7", \
      "interest_picker_0_gfx-hol6-investigative5", "interest_picker_0_gfx-hol6-investigative6", "interest_picker_0_gfx-hol6-investigative7", "interest_picker_0_gfx-hol6-investigative8", \
      'interest_picker_0_h6_artistic', 'interest_picker_0_h6_conventional', 'interest_picker_0_h6_enterprising', 'interest_picker_0_h6_investigative',  'interest_picker_0_h6_realistic',  'interest_picker_0_h6_social', \
      'interest_picker_0_num_moves', 'interest_picker_0_returned_option', 'interest_picker_0_total_time', 'interest_picker_0_time_levels', 'interest_picker_0_total_items_maxlength']


    survey_cols = ['survey_sum_artistic', 'survey_sum_conventional', 'survey_sum_enterprising', 'survey_sum_investigative',  'survey_sum_realistic', 'survey_sum_social']
    _sumvars_ip = ['interest_picker_0_h6_artistic', 'interest_picker_0_h6_conventional', 'interest_picker_0_h6_enterprising', 'interest_picker_0_h6_investigative', 'interest_picker_0_h6_realistic', 'interest_picker_0_h6_social']

    interest_picker_db = pd.DataFrame(merged_person_result_v5, columns = [circle_cols + all_holland6_vars + _sumvars_ip + survey_cols]).fillna(0)

    interest_picker_db_only_pick_vars = pd.DataFrame(merged_person_result_v5, columns = [interest_pick_cols + survey_cols])


    _sumvars_ip = ['interest_picker_0_h6_artistic', 'interest_picker_0_h6_conventional', 'interest_picker_0_h6_enterprising', 'interest_picker_0_h6_investigative', 'interest_picker_0_h6_realistic', 'interest_picker_0_h6_social']
    _sur_vars_ip = ['survey_1_holl6_Artistic', 'survey_1_holl6_Conventional', 'survey_1_holl6_Enterprising', 'survey_1_holl6_Investigative',  'survey_1_holl6_Realistic','survey_1_holl6_Social']
    _circ_a_ip = ['circles_test_4_Creative_rank', 'circles_test_4_Detail-Oriented_rank', 'circles_test_4_Inquisitive_rank', 'circles_test_4_Hands-on_rank', 'circles_test_4_Persuasive_rank', 'circles_test_4_Helpful_rank']
    _circ_b_ip = ['circles_test_4_Creative_size', 'circles_test_4_Detail-Oriented_size', 'circles_test_4_Inquisitive_size', 'circles_test_4_Hands-on_size', 'circles_test_4_Persuasive_size', 'circles_test_4_Helpful_size']
    _circ_c_ip = ['circles_test_4_Creative_standard_distance', 'circles_test_4_Detail-Oriented_standard_distance', 'circles_test_4_Inquisitive_standard_distance', 'circles_test_4_Hands-on_standard_distance', 'circles_test_4_Persuasive_standard_distance', 'circles_test_4_Helpful_standard_distance']

    interest_picker_db_corr = pd.DataFrame(interest_picker_db, columns = [_sumvars_ip + survey_cols + _circ_a_ip + _circ_b_ip + _circ_c_ip], dtype = 'float64')
    a = interest_picker_db_corr.corr()
    a.to_csv('_interest_picker_v14.csv')


    interest_picker_db_corr_2 = pd.DataFrame(interest_picker_db, columns = [all_holland6_vars], dtype = 'float64')
    a_val = interest_picker_db_corr_2.apply(lambda x: pd.value_counts(x))
    a_val.to_csv('_value_counts.csv')


    interest_picker_db_corr = pd.DataFrame(interest_picker_db, columns = [all_holland6_vars + survey_cols + _circ_a_ip + _circ_b_ip + _circ_c_ip], dtype = 'float64')
    a = interest_picker_db_corr.corr()
    a.to_csv('_interest_picker_v22.csv')

    interest_picker_db_15 = interest_picker_db[interest_picker_db['interest_picker_0_total_items_maxlength'] <= 15]
    interest_picker_db_15_corr = pd.DataFrame(interest_picker_db_15, columns = [_sumvars_ip + _circ_a_ip + _circ_b_ip + _circ_c_ip], dtype = 'float64')
    a = interest_picker_db_15_corr.corr()
    a.to_csv('_interest_picker_v33.csv')


    _sumvars_ip_rank = ['interest_picker_0_h6_Artistic_rank', 'interest_picker_0_h6_Conventional_rank', 'interest_picker_0_h6_Enterprising_rank', 'interest_picker_0_h6_Investigative_rank', 'interest_picker_0_h6_Realistic_rank', 'interest_picker_0_h6_Social_rank']
    merged_person_result_v5['interest_picker_0_h6_Social_rank'] = pd.DataFrame(merged_person_result_v5, columns=['interest_picker_0_Helpful_rank', 'interest_picker_0_Caring_rank', "interest_picker_0_gfx-hol6-social1_rank", "interest_picker_0_gfx-hol6-social2_rank", "interest_picker_0_gfx-hol6-social3_rank", "interest_picker_0_gfx-hol6-social5_rank"]).fillna(0).sum(axis = 1)
    merged_person_result_v5['interest_picker_0_h6_Enterprising_rank'] = pd.DataFrame(merged_person_result_v5, columns=['interest_picker_0_Passionate_rank', 'interest_picker_0_Persuasive_rank', "interest_picker_0_gfx-hol6-enterprising2_rank", "interest_picker_0_gfx-hol6-enterprising6_rank", "interest_picker_0_gfx-hol6-enterprising7_rank", "interest_picker_0_gfx-hol6-investigative4_rank"]).fillna(0).sum(axis = 1)
    merged_person_result_v5['interest_picker_0_h6_Realistic_rank'] = pd.DataFrame(merged_person_result_v5, columns=['interest_picker_0_Hands-on_rank', 'interest_picker_0_Mechanical_rank', "interest_picker_0_gfx-hol6-realistic2_rank", "interest_picker_0_gfx-hol6-realistic6_rank", "interest_picker_0_gfx-hol6-realistic7_rank", "interest_picker_0_gfx-hol6-enterprising3_rank"]).fillna(0).sum(axis = 1)
    merged_person_result_v5['interest_picker_0_h6_Artistic_rank'] = pd.DataFrame(merged_person_result_v5, columns=['interest_picker_0_Creative_rank', 'interest_picker_0_Intuitive_rank', "interest_picker_0_gfx-hol6-artistic4_rank", "interest_picker_0_gfx-hol6-artistic5_rank", "interest_picker_0_gfx-hol6-artistic6_rank", "interest_picker_0_gfx-hol6-artistic7_rank"]).fillna(0).sum(axis = 1)
    merged_person_result_v5['interest_picker_0_h6_Conventional_rank'] = pd.DataFrame(merged_person_result_v5, columns=['interest_picker_0_Detail-Oriented_rank', 'interest_picker_0_Thorough_rank', "interest_picker_0_gfx-hol6-conventional5_rank", "interest_picker_0_gfx-hol6-conventional6_rank", "interest_picker_0_gfx-hol6-conventional7_rank", "interest_picker_0_gfx-hol6-conventional8_rank"]).fillna(0).sum(axis = 1)
    merged_person_result_v5['interest_picker_0_h6_Investigative_rank'] = pd.DataFrame(merged_person_result_v5, columns=['interest_picker_0_Inquisitive_rank', 'interest_picker_0_Analytical_rank', "interest_picker_0_gfx-hol6-investigative5_rank", "interest_picker_0_gfx-hol6-investigative6_rank", "interest_picker_0_gfx-hol6-investigative7_rank", "interest_picker_0_gfx-hol6-investigative8_rank"]).fillna(0).sum(axis = 1)

    interest_picker_db_corr_rank = pd.DataFrame(merged_person_result_v5, columns = [_sumvars_ip + _sumvars_ip_rank + _circ_a_ip + _circ_b_ip + _circ_c_ip], dtype = 'float64')
    a = interest_picker_db_corr_rank.corr()
    a.to_csv('_interest_picker_v4.csv')

    ## compare slower with faster times
    interest_picker_db_time28 = interest_picker_db[interest_picker_db['interest_picker_0_time_levels']>=28.00]
    interest_picker_db_time28_corr = pd.DataFrame(interest_picker_db_time28, columns = [_sur_vars_ip + _sumvars_ip + _circ_a_ip + _circ_b_ip + _circ_c_ip], dtype = 'float64')
    atime = interest_picker_db_time28_corr.corr()
    atime.to_csv('_interest_picker_time28.csv')

    ## split words and symbols
    _sumvars_ip_new = ['Artistic_new', 'Conventional_new', 'Enterprising_new', 'Investigative_new', 'Realistic_new', 'Social_new']
    interest_picker_db['Social_new'] = (interest_picker_db['interest_picker_0_Helpful'] + interest_picker_db['interest_picker_0_Caring'] + interest_picker_db['interest_picker_0_gfx-hol6-social1'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-social2'] + interest_picker_db['interest_picker_0_gfx-hol6-investigative3'])

    interest_picker_db['Enterprising_new'] = (interest_picker_db['interest_picker_0_Passionate'] + interest_picker_db['interest_picker_0_Persuasive'] + interest_picker_db['interest_picker_0_gfx-hol6-artistic1'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-enterprising2'] + interest_picker_db['interest_picker_0_gfx-hol6-realistic5'] + interest_picker_db['interest_picker_0_gfx-hol6-investigative4'])

    interest_picker_db['Realistic_new'] = (interest_picker_db['interest_picker_0_Hands-on'] + interest_picker_db['interest_picker_0_Mechanical'] + interest_picker_db['interest_picker_0_gfx-hol6-artistic1'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-artistic2'] + interest_picker_db['interest_picker_0_gfx-hol6-realistic2'] + interest_picker_db['interest_picker_0_gfx-hol6-enterprising3'])

    interest_picker_db['Artistic_new'] = (interest_picker_db['interest_picker_0_Creative'] + interest_picker_db['interest_picker_0_Intuitive'] + interest_picker_db['interest_picker_0_gfx-hol6-enterprising4'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-enterprising5'] + interest_picker_db['interest_picker_0_gfx-hol6-realistic2'] + interest_picker_db['interest_picker_0_gfx-hol6-investigative3'])

    interest_picker_db['Conventional_new'] = (interest_picker_db['interest_picker_0_Detail-Oriented'] + interest_picker_db['interest_picker_0_Thorough'] + interest_picker_db['interest_picker_0_gfx-hol6-enterprising4'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-realistic4'] + interest_picker_db['interest_picker_0_gfx-hol6-investigative3'])

    interest_picker_db['Investigative_new'] = (interest_picker_db['interest_picker_0_Inquisitive'] + interest_picker_db['interest_picker_0_Analytical'] + interest_picker_db['interest_picker_0_gfx-hol6-conventional2'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-enterprising4'] + interest_picker_db['interest_picker_0_gfx-hol6-enterprising5'] + interest_picker_db['interest_picker_0_gfx-hol6-investigative3'])

    interest_picker_db_corr_v5 = pd.DataFrame(interest_picker_db, columns = [_sumvars_ip + _sumvars_ip_new + _circ_a_ip + _circ_b_ip + _circ_c_ip], dtype = 'float64')
    a = interest_picker_db_corr_v5.corr()
    a.to_csv('_interest_picker_v5.csv')

    # PCA
    q1 = pd.DataFrame(interest_picker_db_corr_2, dtype=float64)
    q1_norm = (q1 - q1.mean()) / q1.std()
    q1_s_np = np.array(q1)
    q1_s_np_z = np.nan_to_num(q1_s_np)
    q1_s_np_z_df = pd.DataFrame(q1_s_np_z)

    skfa1 = sklearn.decomposition.FactorAnalysis().fit(q1_s_np_z)
    skpca1 = sklearn.decomposition.PCA().fit(q1_s_np_z)

    _sumvars_ip_words = ['Artistic_words', 'Conventional_words', 'Enterprising_words', 'Investigative_words', 'Realistic_words', 'Social_words']
    _sumvars_ip_sym = ['Artistic_symb', 'Conventional_symb', 'Enterprising_symb', 'Investigative_symb', 'Realistic_symb', 'Social_symb']
    interest_picker_db['Social_words'] = (interest_picker_db['interest_picker_0_Helpful'] + interest_picker_db['interest_picker_0_Caring'])
    interest_picker_db['Social_symb'] = (interest_picker_db['interest_picker_0_gfx-hol6-social1'] + interest_picker_db['interest_picker_0_gfx-hol6-social2'] + interest_picker_db['interest_picker_0_gfx-hol6-social3'] + \
     interest_picker_db['interest_picker_0_gfx-hol6-social4'])

    interest_picker_db['Enterprising_words'] = (interest_picker_db['interest_picker_0_Passionate'] + interest_picker_db['interest_picker_0_Persuasive'])
    interest_picker_db['Enterprising_symb'] = (interest_picker_db['interest_picker_0_gfx-hol6-enterprising1'] + interest_picker_db['interest_picker_0_gfx-hol6-enterprising2'] + interest_picker_db['interest_picker_0_gfx-hol6-enterprising3'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-enterprising4'] + interest_picker_db['interest_picker_0_gfx-hol6-enterprising5'])

    interest_picker_db['Realistic_words'] = (interest_picker_db['interest_picker_0_Hands-on'] + interest_picker_db['interest_picker_0_Mechanical'])
    interest_picker_db['Realistic_symb'] = (interest_picker_db['interest_picker_0_gfx-hol6-realistic1'] + interest_picker_db['interest_picker_0_gfx-hol6-realistic2'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-realistic3'] + interest_picker_db['interest_picker_0_gfx-hol6-realistic4'] + interest_picker_db['interest_picker_0_gfx-hol6-realistic5'])

    interest_picker_db['Artistic_words'] = (interest_picker_db['interest_picker_0_Creative'] + interest_picker_db['interest_picker_0_Intuitive'])
    interest_picker_db['Artistic_symb'] = (interest_picker_db['interest_picker_0_gfx-hol6-artistic1'] + interest_picker_db['interest_picker_0_gfx-hol6-artistic2'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-artistic3'] + interest_picker_db['interest_picker_0_gfx-hol6-artistic4'])

    interest_picker_db['Conventional_words'] = (interest_picker_db['interest_picker_0_Detail-Oriented'] + interest_picker_db['interest_picker_0_Thorough'])
    interest_picker_db['Conventional_symb'] = (interest_picker_db['interest_picker_0_gfx-hol6-conventional1'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-conventional2'] + interest_picker_db['interest_picker_0_gfx-hol6-conventional3'] + interest_picker_db['interest_picker_0_gfx-hol6-conventional4'])

    interest_picker_db['Investigative_words'] = (interest_picker_db['interest_picker_0_Inquisitive'] + interest_picker_db['interest_picker_0_Analytical'])
    interest_picker_db['Investigative_symb'] = (interest_picker_db['interest_picker_0_gfx-hol6-investigative1'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-investigative2'] + interest_picker_db['interest_picker_0_gfx-hol6-investigative3'] + interest_picker_db['interest_picker_0_gfx-hol6-investigative4'])


    interest_picker_db_corr_v2 = pd.DataFrame(interest_picker_db, columns = [_sumvars_ip + _sumvars_ip_words + _sumvars_ip_sym + _circ_a_ip + _circ_b_ip + _circ_c_ip], dtype = 'float64')
    a = interest_picker_db_corr_v2.corr()
    a.to_csv('_interest_picker_v2.csv')


  def check_image_picker_game():

    def score_survey():
      artistic_survey = ['survey_1_holl6_Artistic1', 'survey_1_holl6_Artistic2', 'survey_2_holl6_Artistic3', 'survey_2_holl6_Artistic4', 'survey_3_holl6_Artistic5', 'survey_3_holl6_Artistic6', 'survey_4_holl6_Artistic7', \
        'survey_4_holl6_Artistic8', 'survey_5_holl6_Artistic9', 'survey_5_holl6_Artistic10']
      social_survey = ['survey_1_holl6_Social1', 'survey_1_holl6_Social2', 'survey_2_holl6_Social3', 'survey_2_holl6_Social4', 'survey_3_holl6_Social5', 'survey_3_holl6_Social6', 'survey_4_holl6_Social7', \
        'survey_4_holl6_Social8', 'survey_5_holl6_Social9', 'survey_5_holl6_Social10']
      investigative_survey = ['survey_1_holl6_Investigative1', 'survey_1_holl6_Investigative2', 'survey_2_holl6_Investigative3', 'survey_2_holl6_Investigative4', 'survey_3_holl6_Investigative5', 'survey_3_holl6_Investigative6', 'survey_4_holl6_Investigative7', \
        'survey_4_holl6_Investigative8', 'survey_5_holl6_Investigative9', 'survey_5_holl6_Investigative10']
      conventional_survey = ['survey_1_holl6_Conventional1', 'survey_1_holl6_Conventional2', 'survey_2_holl6_Conventional3', 'survey_2_holl6_Conventional4', 'survey_3_holl6_Conventional5', 'survey_3_holl6_Conventional6', 'survey_4_holl6_Conventional7', \
        'survey_4_holl6_Conventional8', 'survey_5_holl6_Conventional9', 'survey_5_holl6_Conventional10']
      realistic_survey = ['survey_1_holl6_Realistic1', 'survey_1_holl6_Realistic2', 'survey_2_holl6_Realistic3', 'survey_2_holl6_Realistic4', 'survey_3_holl6_Realistic5', 'survey_3_holl6_Realistic6', 'survey_4_holl6_Realistic7', \
        'survey_4_holl6_Realistic8', 'survey_5_holl6_Realistic9', 'survey_5_holl6_Realistic10']
      enterprising_survey = ['survey_1_holl6_Enterprising1', 'survey_1_holl6_Enterprising2', 'survey_2_holl6_Enterprising3', 'survey_2_holl6_Enterprising4', 'survey_3_holl6_Enterprising5', 'survey_3_holl6_Enterprising6', 'survey_4_holl6_Enterprising7', \
        'survey_4_holl6_Enterprising8', 'survey_5_holl6_Enterprising9', 'survey_5_holl6_Enterprising10']
      all_survey = artistic_survey + social_survey + investigative_survey + conventional_survey + realistic_survey + enterprising_survey

      social_vars = ['interest_picker_0_gfx-hol6-social2','interest_picker_0_gfx-hol6-social3','interest_picker_0_gfx-hol6-social5','interest_picker_0_gfx-hol6-social6', 'interest_picker_0_Caring', 'interest_picker_0_Helpful']
      enterprising_vars = ['interest_picker_0_gfx-hol6-enterprising2','interest_picker_0_gfx-hol6-enterprising6','interest_picker_0_gfx-hol6-enterprising8','interest_picker_0_gfx-hol6-enterprising9', 'interest_picker_0_Passionate',  'interest_picker_0_Persuasive']
      conventional_vars = ['interest_picker_0_gfx-hol6-conventional5','interest_picker_0_gfx-hol6-conventional6','interest_picker_0_gfx-hol6-conventional7','interest_picker_0_gfx-hol6-conventional9', 'interest_picker_0_Detail-Oriented', 'interest_picker_0_Thorough']
      realistic_vars = ['interest_picker_0_gfx-hol6-realistic2','interest_picker_0_gfx-hol6-realistic6','interest_picker_0_gfx-hol6-realistic8','interest_picker_0_gfx-hol6-realistic9', 'interest_picker_0_Hands-on', 'interest_picker_0_Mechanical']
      artistic_vars = ['interest_picker_0_gfx-hol6-artistic4','interest_picker_0_gfx-hol6-artistic5','interest_picker_0_gfx-hol6-artistic7','interest_picker_0_gfx-hol6-artistic8', 'interest_picker_0_Creative', 'interest_picker_0_Intuitive']
      investigative_vars = ['interest_picker_0_gfx-hol6-investigative6','interest_picker_0_gfx-hol6-investigative9','interest_picker_0_gfx-hol6-investigative10','interest_picker_0_gfx-hol6-investigative11', 'interest_picker_0_Analytical', 'interest_picker_0_Inquisitive']
      all_holland6_vars = artistic_vars + social_vars + investigative_vars + conventional_vars + realistic_vars + enterprising_vars

      def add_one(x):
        if x == 1:
          x += 1
          return x
      boost_plus_1 = ['interest_picker_0_gfx-hol6-artistic5', 'interest_picker_0_gfx-hol6-enterprising2', 'interest_picker_0_gfx-hol6-investigative6', 'interest_picker_0_gfx-hol6-realistic6', 'interest_picker_0_gfx-hol6-social5', 'interest_picker_0_gfx-hol6-conventional9']
      for b in boost_plus_1:
        merged_person_result_v5[b] = merged_person_result_v5[b].apply(add_one)


      holland6_vars = ['artistic', 'social', 'investigative', 'conventional', 'realistic', 'enterprising']
      survey_sum_cols = []
      for i in holland6_vars:
        merged_person_result_v5['survey_sum_' + i] = pd.DataFrame(merged_person_result_v5, columns = vars()[i + '_survey']).applymap(lambda x: x - 1).fillna(0).sum(axis = 1)
        merged_person_result_v5['interest_picker_0_h6_' + i] = pd.DataFrame(merged_person_result_v5, columns = vars()[i + '_vars']).fillna(0).sum(axis = 1)
        survey_sum_cols.append('survey_sum_' + i)

      merged_person_result_v5['holland6_trait_survey'] = pd.DataFrame(merged_person_result_v5, columns = survey_sum_cols).idxmax(axis=1)

    circle_cols = ['circles_test_4_Creative_distance',  'circles_test_4_Creative_overlap', 'circles_test_4_Creative_overlap_distance',  'circles_test_4_Creative_rank',  'circles_test_4_Creative_size',  'circles_test_4_Creative_standard_distance', \
      'circles_test_4_Detail-Oriented_distance', 'circles_test_4_Detail-Oriented_overlap',  'circles_test_4_Detail-Oriented_overlap_distance', 'circles_test_4_Detail-Oriented_rank', 'circles_test_4_Detail-Oriented_size', 'circles_test_4_Detail-Oriented_standard_distance', \
      'circles_test_4_Inquisitive_distance', 'circles_test_4_Inquisitive_overlap',  'circles_test_4_Inquisitive_overlap_distance', 'circles_test_4_Inquisitive_rank', 'circles_test_4_Inquisitive_size', 'circles_test_4_Inquisitive_standard_distance', \
      'circles_test_4_Hands-on_distance', 'circles_test_4_Hands-on_overlap', 'circles_test_4_Hands-on_overlap_distance',  'circles_test_4_Hands-on_rank',  'circles_test_4_Hands-on_size',  'circles_test_4_Hands-on_standard_distance', \
      'circles_test_4_Persuasive_distance',  'circles_test_4_Persuasive_overlap', 'circles_test_4_Persuasive_overlap_distance',  'circles_test_4_Persuasive_rank',  'circles_test_4_Persuasive_size',  'circles_test_4_Persuasive_standard_distance', \
      'circles_test_4_Helpful_distance', 'circles_test_4_Helpful_overlap',  'circles_test_4_Helpful_overlap_distance', 'circles_test_4_Helpful_rank', 'circles_test_4_Helpful_size', 'circles_test_4_Helpful_standard_distance',  'circles_test_4_artistic', \
      'circles_test_4_artistic_zscore',  'circles_test_4_conventional', 'circles_test_4_conventional_zscore',  'circles_test_4_enterprising', 'circles_test_4_enterprising_zscore',  'circles_test_4_investigative',  'circles_test_4_investigative_zscore', \
      'circles_test_4_num_moves',  'circles_test_4_realistic',  'circles_test_4_realistic_zscore', 'circles_test_4_social', 'circles_test_4_social_zscore', 'circles_test_4_total_time']


    interest_pick_cols = ['interest_picker_0_Analytical',  'interest_picker_0_Caring',  'interest_picker_0_Creative',  'interest_picker_0_Detail-Oriented', 'interest_picker_0_Hands-on',  'interest_picker_0_Helpful', \
      'interest_picker_0_Inquisitive', 'interest_picker_0_Intuitive', 'interest_picker_0_Mechanical',  'interest_picker_0_Passionate',  'interest_picker_0_Persuasive',  'interest_picker_0_Thorough',  \
      "interest_picker_0_gfx-hol6-social1", "interest_picker_0_gfx-hol6-social2", "interest_picker_0_gfx-hol6-social3", "interest_picker_0_gfx-hol6-social5", \
      "interest_picker_0_gfx-hol6-enterprising2", "interest_picker_0_gfx-hol6-enterprising6", "interest_picker_0_gfx-hol6-enterprising7", "interest_picker_0_gfx-hol6-investigative4", \
      "interest_picker_0_gfx-hol6-conventional5", "interest_picker_0_gfx-hol6-conventional6", "interest_picker_0_gfx-hol6-conventional7", "interest_picker_0_gfx-hol6-conventional8", \
      "interest_picker_0_gfx-hol6-realistic2", "interest_picker_0_gfx-hol6-realistic6", "interest_picker_0_gfx-hol6-realistic7", "interest_picker_0_gfx-hol6-enterprising3", \
      "interest_picker_0_gfx-hol6-artistic4", "interest_picker_0_gfx-hol6-artistic5", "interest_picker_0_gfx-hol6-artistic6", "interest_picker_0_gfx-hol6-artistic7", \
      "interest_picker_0_gfx-hol6-investigative5", "interest_picker_0_gfx-hol6-investigative6", "interest_picker_0_gfx-hol6-investigative7", "interest_picker_0_gfx-hol6-investigative8", \
      'interest_picker_0_h6_artistic', 'interest_picker_0_h6_conventional', 'interest_picker_0_h6_enterprising', 'interest_picker_0_h6_investigative',  'interest_picker_0_h6_realistic',  'interest_picker_0_h6_social', \
      'interest_picker_0_num_moves', 'interest_picker_0_returned_option', 'interest_picker_0_total_time', 'interest_picker_0_time_levels', 'interest_picker_0_total_items_maxlength']


    survey_cols = ['survey_sum_artistic', 'survey_sum_conventional', 'survey_sum_enterprising', 'survey_sum_investigative',  'survey_sum_realistic', 'survey_sum_social']
    _sumvars_ip = ['interest_picker_0_h6_artistic', 'interest_picker_0_h6_conventional', 'interest_picker_0_h6_enterprising', 'interest_picker_0_h6_investigative', 'interest_picker_0_h6_realistic', 'interest_picker_0_h6_social']

    interest_picker_db = pd.DataFrame(merged_person_result_v5, columns = [circle_cols + all_holland6_vars + _sumvars_ip + survey_cols]).fillna(0)

    interest_picker_db_only_pick_vars = pd.DataFrame(merged_person_result_v5, columns = [interest_pick_cols + survey_cols])


    _sumvars_ip = ['interest_picker_0_h6_artistic', 'interest_picker_0_h6_conventional', 'interest_picker_0_h6_enterprising', 'interest_picker_0_h6_investigative', 'interest_picker_0_h6_realistic', 'interest_picker_0_h6_social']
    _sur_vars_ip = ['survey_1_holl6_Artistic', 'survey_1_holl6_Conventional', 'survey_1_holl6_Enterprising', 'survey_1_holl6_Investigative',  'survey_1_holl6_Realistic','survey_1_holl6_Social']
    _circ_a_ip = ['circles_test_4_Creative_rank', 'circles_test_4_Detail-Oriented_rank', 'circles_test_4_Inquisitive_rank', 'circles_test_4_Hands-on_rank', 'circles_test_4_Persuasive_rank', 'circles_test_4_Helpful_rank']
    _circ_b_ip = ['circles_test_4_Creative_size', 'circles_test_4_Detail-Oriented_size', 'circles_test_4_Inquisitive_size', 'circles_test_4_Hands-on_size', 'circles_test_4_Persuasive_size', 'circles_test_4_Helpful_size']
    _circ_c_ip = ['circles_test_4_Creative_standard_distance', 'circles_test_4_Detail-Oriented_standard_distance', 'circles_test_4_Inquisitive_standard_distance', 'circles_test_4_Hands-on_standard_distance', 'circles_test_4_Persuasive_standard_distance', 'circles_test_4_Helpful_standard_distance']

    interest_picker_db_corr = pd.DataFrame(interest_picker_db, columns = [_sumvars_ip + survey_cols + _circ_a_ip + _circ_b_ip + _circ_c_ip], dtype = 'float64')
    a = interest_picker_db_corr.corr()
    a.to_csv('_interest_picker_v14.csv')

    interest_picker_db_corr_2 = pd.DataFrame(interest_picker_db, columns = [all_holland6_vars], dtype = 'float64')
    a_val = interest_picker_db_corr_2.apply(lambda x: pd.value_counts(x))
    a_val.to_csv('_value_counts.csv')

    interest_picker_db_corr = pd.DataFrame(interest_picker_db, columns = [all_holland6_vars + survey_cols + _circ_a_ip + _circ_b_ip + _circ_c_ip], dtype = 'float64')
    a = interest_picker_db_corr.corr()
    a.to_csv('_interest_picker_v22.csv')

    interest_picker_db_15 = interest_picker_db[interest_picker_db['interest_picker_0_total_items_maxlength'] <= 15]
    interest_picker_db_15_corr = pd.DataFrame(interest_picker_db_15, columns = [_sumvars_ip + _circ_a_ip + _circ_b_ip + _circ_c_ip], dtype = 'float64')
    a = interest_picker_db_15_corr.corr()
    a.to_csv('_interest_picker_v33.csv')


    _sumvars_ip_rank = ['interest_picker_0_h6_Artistic_rank', 'interest_picker_0_h6_Conventional_rank', 'interest_picker_0_h6_Enterprising_rank', 'interest_picker_0_h6_Investigative_rank', 'interest_picker_0_h6_Realistic_rank', 'interest_picker_0_h6_Social_rank']
    merged_person_result_v5['interest_picker_0_h6_Social_rank'] = pd.DataFrame(merged_person_result_v5, columns=['interest_picker_0_Helpful_rank', 'interest_picker_0_Caring_rank', "interest_picker_0_gfx-hol6-social1_rank", "interest_picker_0_gfx-hol6-social2_rank", "interest_picker_0_gfx-hol6-social3_rank", "interest_picker_0_gfx-hol6-social5_rank"]).fillna(0).sum(axis = 1)
    merged_person_result_v5['interest_picker_0_h6_Enterprising_rank'] = pd.DataFrame(merged_person_result_v5, columns=['interest_picker_0_Passionate_rank', 'interest_picker_0_Persuasive_rank', "interest_picker_0_gfx-hol6-enterprising2_rank", "interest_picker_0_gfx-hol6-enterprising6_rank", "interest_picker_0_gfx-hol6-enterprising7_rank", "interest_picker_0_gfx-hol6-investigative4_rank"]).fillna(0).sum(axis = 1)
    merged_person_result_v5['interest_picker_0_h6_Realistic_rank'] = pd.DataFrame(merged_person_result_v5, columns=['interest_picker_0_Hands-on_rank', 'interest_picker_0_Mechanical_rank', "interest_picker_0_gfx-hol6-realistic2_rank", "interest_picker_0_gfx-hol6-realistic6_rank", "interest_picker_0_gfx-hol6-realistic7_rank", "interest_picker_0_gfx-hol6-enterprising3_rank"]).fillna(0).sum(axis = 1)
    merged_person_result_v5['interest_picker_0_h6_Artistic_rank'] = pd.DataFrame(merged_person_result_v5, columns=['interest_picker_0_Creative_rank', 'interest_picker_0_Intuitive_rank', "interest_picker_0_gfx-hol6-artistic4_rank", "interest_picker_0_gfx-hol6-artistic5_rank", "interest_picker_0_gfx-hol6-artistic6_rank", "interest_picker_0_gfx-hol6-artistic7_rank"]).fillna(0).sum(axis = 1)
    merged_person_result_v5['interest_picker_0_h6_Conventional_rank'] = pd.DataFrame(merged_person_result_v5, columns=['interest_picker_0_Detail-Oriented_rank', 'interest_picker_0_Thorough_rank', "interest_picker_0_gfx-hol6-conventional5_rank", "interest_picker_0_gfx-hol6-conventional6_rank", "interest_picker_0_gfx-hol6-conventional7_rank", "interest_picker_0_gfx-hol6-conventional8_rank"]).fillna(0).sum(axis = 1)
    merged_person_result_v5['interest_picker_0_h6_Investigative_rank'] = pd.DataFrame(merged_person_result_v5, columns=['interest_picker_0_Inquisitive_rank', 'interest_picker_0_Analytical_rank', "interest_picker_0_gfx-hol6-investigative5_rank", "interest_picker_0_gfx-hol6-investigative6_rank", "interest_picker_0_gfx-hol6-investigative7_rank", "interest_picker_0_gfx-hol6-investigative8_rank"]).fillna(0).sum(axis = 1)


    interest_picker_db_corr_rank = pd.DataFrame(merged_person_result_v5, columns = [_sumvars_ip + _sumvars_ip_rank + _circ_a_ip + _circ_b_ip + _circ_c_ip], dtype = 'float64')
    a = interest_picker_db_corr_rank.corr()
    a.to_csv('_interest_picker_v4.csv')

    ## compare slower with faster times
    interest_picker_db_time28 = interest_picker_db[interest_picker_db['interest_picker_0_time_levels']>=28.00]
    interest_picker_db_time28_corr = pd.DataFrame(interest_picker_db_time28, columns = [_sur_vars_ip + _sumvars_ip + _circ_a_ip + _circ_b_ip + _circ_c_ip], dtype = 'float64')
    atime = interest_picker_db_time28_corr.corr()
    atime.to_csv('_interest_picker_time28.csv')

    ## split words and symbols
    _sumvars_ip_new = ['Artistic_new', 'Conventional_new', 'Enterprising_new', 'Investigative_new', 'Realistic_new', 'Social_new']
    interest_picker_db['Social_new'] = (interest_picker_db['interest_picker_0_Helpful'] + interest_picker_db['interest_picker_0_Caring'] + interest_picker_db['interest_picker_0_gfx-hol6-social1'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-social2'] + interest_picker_db['interest_picker_0_gfx-hol6-investigative3'])
    interest_picker_db['Enterprising_new'] = (interest_picker_db['interest_picker_0_Passionate'] + interest_picker_db['interest_picker_0_Persuasive'] + interest_picker_db['interest_picker_0_gfx-hol6-artistic1'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-enterprising2'] + interest_picker_db['interest_picker_0_gfx-hol6-realistic5'] + interest_picker_db['interest_picker_0_gfx-hol6-investigative4'])
    interest_picker_db['Realistic_new'] = (interest_picker_db['interest_picker_0_Hands-on'] + interest_picker_db['interest_picker_0_Mechanical'] + interest_picker_db['interest_picker_0_gfx-hol6-artistic1'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-artistic2'] + interest_picker_db['interest_picker_0_gfx-hol6-realistic2'] + interest_picker_db['interest_picker_0_gfx-hol6-enterprising3'])
    interest_picker_db['Artistic_new'] = (interest_picker_db['interest_picker_0_Creative'] + interest_picker_db['interest_picker_0_Intuitive'] + interest_picker_db['interest_picker_0_gfx-hol6-enterprising4'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-enterprising5'] + interest_picker_db['interest_picker_0_gfx-hol6-realistic2'] + interest_picker_db['interest_picker_0_gfx-hol6-investigative3'])
    interest_picker_db['Conventional_new'] = (interest_picker_db['interest_picker_0_Detail-Oriented'] + interest_picker_db['interest_picker_0_Thorough'] + interest_picker_db['interest_picker_0_gfx-hol6-enterprising4'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-realistic4'] + interest_picker_db['interest_picker_0_gfx-hol6-investigative3'])
    interest_picker_db['Investigative_new'] = (interest_picker_db['interest_picker_0_Inquisitive'] + interest_picker_db['interest_picker_0_Analytical'] + interest_picker_db['interest_picker_0_gfx-hol6-conventional2'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-enterprising4'] + interest_picker_db['interest_picker_0_gfx-hol6-enterprising5'] + interest_picker_db['interest_picker_0_gfx-hol6-investigative3'])

    interest_picker_db_corr_v5 = pd.DataFrame(interest_picker_db, columns = [_sumvars_ip + _sumvars_ip_new + _circ_a_ip + _circ_b_ip + _circ_c_ip], dtype = 'float64')
    a = interest_picker_db_corr_v5.corr()
    a.to_csv('_interest_picker_v5.csv')

    # PCA
    q1 = pd.DataFrame(interest_picker_db_corr_2, dtype=float64)
    q1_norm = (q1 - q1.mean()) / q1.std()
    q1_s_np = np.array(q1)
    q1_s_np_z = np.nan_to_num(q1_s_np)
    q1_s_np_z_df = pd.DataFrame(q1_s_np_z)

    skfa1 = sklearn.decomposition.FactorAnalysis().fit(q1_s_np_z)
    skpca1 = sklearn.decomposition.PCA().fit(q1_s_np_z)

    _sumvars_ip_words = ['Artistic_words', 'Conventional_words', 'Enterprising_words', 'Investigative_words', 'Realistic_words', 'Social_words']
    _sumvars_ip_sym = ['Artistic_symb', 'Conventional_symb', 'Enterprising_symb', 'Investigative_symb', 'Realistic_symb', 'Social_symb']
    interest_picker_db['Social_words'] = (interest_picker_db['interest_picker_0_Helpful'] + interest_picker_db['interest_picker_0_Caring'])
    interest_picker_db['Social_symb'] = (interest_picker_db['interest_picker_0_gfx-hol6-social1'] + interest_picker_db['interest_picker_0_gfx-hol6-social2'] + interest_picker_db['interest_picker_0_gfx-hol6-social3'] + \
     interest_picker_db['interest_picker_0_gfx-hol6-social4'])

    interest_picker_db['Enterprising_words'] = (interest_picker_db['interest_picker_0_Passionate'] + interest_picker_db['interest_picker_0_Persuasive'])
    interest_picker_db['Enterprising_symb'] = (interest_picker_db['interest_picker_0_gfx-hol6-enterprising1'] + interest_picker_db['interest_picker_0_gfx-hol6-enterprising2'] + interest_picker_db['interest_picker_0_gfx-hol6-enterprising3'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-enterprising4'] + interest_picker_db['interest_picker_0_gfx-hol6-enterprising5'])

    interest_picker_db['Realistic_words'] = (interest_picker_db['interest_picker_0_Hands-on'] + interest_picker_db['interest_picker_0_Mechanical'])
    interest_picker_db['Realistic_symb'] = (interest_picker_db['interest_picker_0_gfx-hol6-realistic1'] + interest_picker_db['interest_picker_0_gfx-hol6-realistic2'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-realistic3'] + interest_picker_db['interest_picker_0_gfx-hol6-realistic4'] + interest_picker_db['interest_picker_0_gfx-hol6-realistic5'])

    interest_picker_db['Artistic_words'] = (interest_picker_db['interest_picker_0_Creative'] + interest_picker_db['interest_picker_0_Intuitive'])
    interest_picker_db['Artistic_symb'] = (interest_picker_db['interest_picker_0_gfx-hol6-artistic1'] + interest_picker_db['interest_picker_0_gfx-hol6-artistic2'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-artistic3'] + interest_picker_db['interest_picker_0_gfx-hol6-artistic4'])

    interest_picker_db['Conventional_words'] = (interest_picker_db['interest_picker_0_Detail-Oriented'] + interest_picker_db['interest_picker_0_Thorough'])
    interest_picker_db['Conventional_symb'] = (interest_picker_db['interest_picker_0_gfx-hol6-conventional1'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-conventional2'] + interest_picker_db['interest_picker_0_gfx-hol6-conventional3'] + interest_picker_db['interest_picker_0_gfx-hol6-conventional4'])

    interest_picker_db['Investigative_words'] = (interest_picker_db['interest_picker_0_Inquisitive'] + interest_picker_db['interest_picker_0_Analytical'])
    interest_picker_db['Investigative_symb'] = (interest_picker_db['interest_picker_0_gfx-hol6-investigative1'] + \
      interest_picker_db['interest_picker_0_gfx-hol6-investigative2'] + interest_picker_db['interest_picker_0_gfx-hol6-investigative3'] + interest_picker_db['interest_picker_0_gfx-hol6-investigative4'])


    interest_picker_db_corr_v2 = pd.DataFrame(interest_picker_db, columns = [_sumvars_ip + _sumvars_ip_words + _sumvars_ip_sym + _circ_a_ip + _circ_b_ip + _circ_c_ip], dtype = 'float64')
    a = interest_picker_db_corr_v2.corr()
    a.to_csv('_interest_picker_v2.csv')


  def check_image_data():

    image_data = pd.DataFrame(all_part1)

    user_tipi = ['Extraversion_tipi', 'Conscientiousness_tipi', 'Emotional_Stability_tipi', 'Agreeableness_tipi', 'Openness_tipi']

    user_tipi_pos = ['survey_tipi_extraverted', 'survey_tipi_dependable', 'survey_tipi_calm', 'survey_tipi_sympathetic', 'survey_tipi_open']
    user_tipi_neg = ['survey_tipi_reserved', 'survey_tipi_disorganized', 'survey_tipi_anxious', 'survey_tipi_critical', 'survey_tipi_conventional']

    image_vars_0 = ['image_rank_0_cf:achromatic', 'image_rank_0_cf:anger', 'image_rank_0_cf:color', 'image_rank_0_cf:human', 'image_rank_0_cf:human_eyes', 'image_rank_0_cf:male', 'image_rank_0_cf:man_made', 'image_rank_0_cf:movement',
      'image_rank_0_cf:negative_space', 'image_rank_0_cf:pair', 'image_rank_0_cf:reflection', 'image_rank_0_cf:sadness', 'image_rank_0_cf:shading', 'image_rank_0_cf:texture', 'image_rank_0_cf:whole']

    image_vars_2 = ['image_rank_2_cf:abstraction', 'image_rank_2_cf:animal', 'image_rank_2_cf:animal_eyes', 'image_rank_2_cf:color', 'image_rank_2_cf:human', 'image_rank_2_cf:male',  'image_rank_2_cf:man_made',  'image_rank_2_cf:movement',  'image_rank_2_cf:mystery',
      'image_rank_2_cf:nature',  'image_rank_2_cf:negative_space',  'image_rank_2_cf:pair',  'image_rank_2_cf:reflection',  'image_rank_2_cf:shading', 'image_rank_2_cf:texture', 'image_rank_2_cf:vista', 'image_rank_2_cf:whole']

    image_vars_5 = ['image_rank_5_cf:abstraction', 'image_rank_5_cf:achromatic',  'image_rank_5_cf:animal',  'image_rank_5_cf:animal_eyes', 'image_rank_5_cf:bird',  'image_rank_5_cf:color', 'image_rank_5_cf:happy', 'image_rank_5_cf:human', 'image_rank_5_cf:human_eyes',
      'image_rank_5_cf:movement',  'image_rank_5_cf:mystery', 'image_rank_5_cf:nature',  'image_rank_5_cf:negative_space',  'image_rank_5_cf:pair',  'image_rank_5_cf:reflection',  'image_rank_5_cf:shading', 'image_rank_5_cf:texture', 'image_rank_5_cf:vista',
      'image_rank_5_cf:whole']

    circles_1 = ['circles_test_1_Sociable_', 'circles_test_1_Self-Disciplined_', 'circles_test_1_Anxious_', 'circles_test_1_Cooperative_', 'circles_test_1_Curious_']
    circles_3 = ['circles_test_3_Self-Reflective_', 'circles_test_3_Disorganized_', 'circles_test_3_Calm_', 'circles_test_3_Judgmental_', 'circles_test_3_Traditional_']

    circle_vars = ['overlap', 'rank', 'size', 'standard_distance']

    big5_vars_all = []
    for n, i in enumerate(circles_1):
      for v in circle_vars:
        big5_vars_all.append(i + v)
        big5_vars_all.append(circles_3[n] + v)


    testing_images = pd.DataFrame(image_data, columns = [user_tipi + user_tipi_pos + user_tipi_neg + image_vars_5], dtype = 'float64')
    a = testing_images.corr()
    a.to_csv('image_5_corr.xls')

    testing_images_circles = pd.DataFrame(image_data, columns = [image_vars_5 + big5_vars_all], dtype = 'float64')
    a = testing_images_circles.corr()
    a.to_csv('image_circ_5_corr.xls')


    im_rank_0 = ['image_rank_0_rank_0', 'image_rank_0_rank_1', 'image_rank_0_rank_0', 'image_rank_0_rank_3', 'image_rank_0_rank_4']
    im_rank_2 = ['image_rank_2_rank_0', 'image_rank_2_rank_1', 'image_rank_2_rank_2', 'image_rank_2_rank_3', 'image_rank_2_rank_4']
    im_rank_5 = ['image_rank_5_rank_0', 'image_rank_5_rank_1', 'image_rank_5_rank_5', 'image_rank_5_rank_3', 'image_rank_5_rank_4']

    def use_rank_for_images(row):
      #row[]
      pass

    for n, i in enumerate(im_rank_0):
      #image_data['image_rank_F2a'] = image_data.apply( use_rank_for_images, axis = 1 )
      print i
      for let in ['a', 'b', 'c', 'd', 'e']:
        if image_data[i] == 'F2' + let:
          image_data['image_rank_F2' + let] = n


      # image_rank_F2a = rank_num
      # image_rank_F2b
      # image_rank_F2c
      # image_rank_F2d
      # image_rank_F2e


        # make uniform survey tipi var
        # def fix_tipi_survey(df):
        #   def my_test2(row):
        #     if np.isnan(row['survey_3' + i]):
        #       return row['survey_7' + i]
        #     elif np.isnan(row['survey_7' + i]):
        #       return row['survey_3' + i]

        #   for i in ['_tipi_extraverted', '_tipi_reserved', '_tipi_sympathetic', '_tipi_critical', '_tipi_dependable','_tipi_disorganized', '_tipi_calm','_tipi_anxious', '_tipi_open','_tipi_conventional']:
        #     df['survey' + i] = df.apply(my_test2, axis=1)
        #   return df
        # merged_person_result_v2 = fix_tipi_survey(merged_person_result_v2)


    testing_image_rank = pd.DataFrame(image_data, columns = [user_tipi + user_tipi_pos + user_tipi_neg + im_rank_0 + im_rank_2 + im_rank_5], dtype = 'float64')
    a = testing_image_rank.corr()
    a.to_csv('image_rank_corr.xls')


  def check_scores_data_holl6():
    holl6_db = pd.DataFrame(merged_person_result_v3)
    user_hol6 = ['survey_holl6_Realistic', 'survey_holl6_Investigative', 'survey_holl6_Artistic', 'survey_holl6_Social', 'survey_holl6_Enterprising', 'survey_holl6_Conventional']
    calc_holl6 = ['circles_test_4_realistic', 'circles_test_4_investigative', 'circles_test_4_artistic', 'circles_test_4_social', 'circles_test_4_enterprising', 'circles_test_4_conventional']
    holl6_circles = ['circles_test_4_Hands-on_', 'circles_test_4_Inquisitive_', 'circles_test_4_Creative_', 'circles_test_4_Helpful_', 'circles_test_4_Persuasive_', 'circles_test_4_Detail-Oriented_']
    circle_vars = ['overlap', 'rank', 'size', 'standard_distance']

    for v in circle_vars:
      vars()[v + '_holl_uno'] = []
      vars()[v + '_holl_uno_sc'] = []
    for i in holl6_circles:
      vars()[i + 'holl_sc'] = []

    holl6_vars_all = []
    for n, i in enumerate(holl6_circles):
      holl6_vars_all.append(user_hol6[n])
      for v in circle_vars:
        holl6_vars_all.append(i + v)
        vars()[v + '_holl_uno'].append(i + v)
        vars()[v + '_holl_uno_sc'].append(i + v + '_sc')
        vars()[i + 'holl_sc'].append(i + v + '_sc')

    for v in circle_vars:
      holl6_db['min_val'+v] = pd.DataFrame(holl6_db, columns = vars()[v + '_holl_uno']).astype(np.float64).min(axis=1)
      holl6_db['max_val'+v] = pd.DataFrame(holl6_db, columns = vars()[v + '_holl_uno']).astype(np.float64).max(axis=1)
      for k in vars()[v + '_holl_uno']:
        holl6_db[k + '_sc'] = (np.float64(holl6_db[k]) - holl6_db['min_val'+v]) / ( holl6_db['max_val'+v] - holl6_db['min_val'+v] )
    for n, i in enumerate(user_hol6):
      _df = holl6_db[holl6_db[user_hol6[n]].notnull()]
      _df2 = pd.DataFrame(_df, columns = holl6_vars_all)
      _df2 = sm.add_constant(_df2, prepend=False)
      _df2_ft = np.array(_df2, dtype='float64')
      _df_a = np.array(_df[user_hol6[n]], dtype='float64')
      B,SE,PVAL,INMODEL,STATS,NEXTSTEP,HISTORY = stepwisefit(_df2_ft, _df_a)
      print INMODEL, "\n", PVAL
      print "\n\n"
    testing_holl6 = pd.DataFrame(holl6_db, columns = holl6_vars_all, dtype = 'float64' )


  def check_scores_data_big5():
    big5_db = pd.DataFrame(merged_person_result_v3)
    big5_cols = ['extraversion', 'conscientiousness', 'neuroticism', 'agreeableness', 'openness']
    user_tipi = ['Extraversion_tipi', 'Conscientiousness_tipi', 'Emotional_Stability_tipi', 'Agreeableness_tipi', 'Openness_tipi']
    user_tipi_pos = ['survey_tipi_extraverted', 'survey_tipi_dependable', 'survey_tipi_calm', 'survey_tipi_sympathetic', 'survey_tipi_open']
    user_tipi_neg = ['survey_tipi_reserved', 'survey_tipi_disorganized', 'survey_tipi_anxious', 'survey_tipi_critical', 'survey_tipi_conventional']
    calc_holl6 = ['circles_test_4_realistic', 'circles_test_4_investigative', 'circles_test_4_artistic', 'circles_test_4_social', 'circles_test_4_enterprising', 'circles_test_4_conventional']
    circles_1 = ['circles_test_1_Sociable_', 'circles_test_1_Self-Disciplined_', 'circles_test_1_Anxious_', 'circles_test_1_Cooperative_', 'circles_test_1_Curious_']
    circles_3 = ['circles_test_3_Self-Reflective_', 'circles_test_3_Disorganized_', 'circles_test_3_Calm_', 'circles_test_3_Judgmental_', 'circles_test_3_Traditional_']
    circle_vars = ['overlap', 'rank', 'size', 'standard_distance']

    for v in circle_vars:
      vars()[v + '_big5_uno1'] = []
      vars()[v + '_big5_uno1_sc'] = []
      vars()[v + '_big5_uno3'] = []
      vars()[v + '_big5_uno3_sc'] = []
    for i in circles_1:
      vars()[i + 'big5_sc'] = []
    for i in circles_3:
      vars()[i + 'big5_sc'] = []

    big5_vars_all = []
    for n, i in enumerate(circles_1):
      big5_vars_all.append(user_tipi_pos[n])
      big5_vars_all.append(user_tipi_neg[n])
      big5_vars_all.append(user_tipi[n])
      for v in circle_vars:
        big5_vars_all.append(i + v)
        big5_vars_all.append(circles_3[n] + v)
        vars()[v + '_big5_uno1'].append(i + v)
        vars()[v + '_big5_uno1_sc'].append(i + v + '_sc')
        vars()[i + 'big5_sc'].append(i + v)
        vars()[v + '_big5_uno3'].append(circles_3[n] + v)
        vars()[v + '_big5_uno3_sc'].append(circles_3[n] + v + '_sc')
        vars()[i + 'big5_sc'].append(circles_3[n] + v)

    for v in circle_vars:
      big5_db['min_val'+v] = pd.DataFrame(big5_db, columns = vars()[v + '_big5_uno1']).astype(np.float64).min(axis=1)
      big5_db['max_val'+v] = pd.DataFrame(big5_db, columns = vars()[v + '_big5_uno1']).astype(np.float64).max(axis=1)
      big5_db['min_val'+v] = pd.DataFrame(big5_db, columns = vars()[v + '_big5_uno3']).astype(np.float64).min(axis=1)
      big5_db['max_val'+v] = pd.DataFrame(big5_db, columns = vars()[v + '_big5_uno3']).astype(np.float64).max(axis=1)
      for k in vars()[v + '_big5_uno1']:
        big5_db[k + '_sc'] = (np.float64(big5_db[k]) - big5_db['min_val'+v]) / ( big5_db['max_val'+v] - big5_db['min_val'+v] )
      for k in vars()[v + '_big5_uno3']:
        big5_db[k + '_sc'] = (np.float64(big5_db[k]) - big5_db['min_val'+v]) / ( big5_db['max_val'+v] - big5_db['min_val'+v] )
    # stewise
    for n, i in enumerate(circles_1):
      print "\n\n\n", i
      _df_orig = pd.DataFrame(big5_db, columns = [user_tipi[n]] + vars()[i + 'big5_sc'] + [user_tipi_pos[n]] + [user_tipi_neg[n]], dtype = 'float64')
      _df = _df_orig[_df_orig[user_tipi_pos[n]].notnull()]
      _2df = _df[_df[user_tipi_neg[n]].notnull()]
      _3df = pd.DataFrame(_2df, columns = [user_tipi_pos[n], user_tipi_neg[n]],dtype = 'float64')
      _df2 = pd.DataFrame(_2df, columns = vars()[i + 'big5_sc'], dtype = 'float64')
      _df2 = sm.add_constant(_df2, prepend=False)
      _df2 = _df2.fillna(0)
      results = sm.OLS(_3df[user_tipi_pos[n]], _df2).fit()
      print results.summary(), "\n"
      results = sm.OLS(_3df[user_tipi_neg[n]], _df2).fit()
      print results.summary(), "\n"
      #stepwise fit
      _df2_ft = np.array(_df2, dtype='float64')
      _df_a = np.array(_3df[user_tipi_pos[n]], dtype='float64')
      B,SE,PVAL,INMODEL,STATS,NEXTSTEP,HISTORY = stepwisefit(_df2_ft, _df_a)
      print INMODEL, "\n", PVAL
      print "\n\n"
      _df_a = np.array(_3df[user_tipi_neg[n]], dtype='float64')
      B,SE,PVAL,INMODEL,STATS,NEXTSTEP,HISTORY = stepwisefit(_df2_ft, _df_a)
      print INMODEL, "\n", PVAL
      print "\n\n"
    testing_big5 = pd.DataFrame(big5_db, columns = big5_vars_all, dtype = 'float64' )

    a = testing_big5.corr()
    a.to_csv('big5_corr.xls')

    def pca_and_fa(df, col_vars):
      q1 = pd.DataFrame(df, columns=col_vars, dtype=float64)
      q1_norm = (q1 - q1.mean()) / q1.std()
      q1_s_np = np.array(q1_norm)
      q1_s_np_z = np.nan_to_num(q1_s_np)
      q1_s_np_z_df = pd.DataFrame(q1_s_np_z)
      skfa1 = sklearn.decomposition.FactorAnalysis().fit(q1_s_np_z)
      skpca1 = sklearn.decomposition.PCA().fit(q1_s_np_z)
      return skpca1, skfa1

    for i in holl6_circles:
      i = 'circles_test_4_Mechanical_'
      pca1, fa1 = pca_and_fa(holl6_db, vars()[i + 'holl_sc'])
      print pca1.components_

    df_pcar, df_fa = working_with_rpy(q1_s_np_z_df, 2, rotation = "promax")

    def working_with_rpy(dataframe_pca, num_facs, rotation = "none"):
      import pandas.rpy.common as com
      import rpy2.robjects as robjects
      from rpy2.robjects.packages import importr
      psych = importr('psych')
      r_dataframe = com.convert_to_r_dataframe(dataframe_pca)
      pca = psych.principal(r_dataframe, nfactors=num_facs, rotate=rotation, score=True)
      fa = psych.fa(r_dataframe, nfactors = num_facs, rotate = rotation, fm = "pa")
      pca_loadings = pd.DataFrame(np.array(pca[4]))
      fa_loadings = pd.DataFrame(np.array(fa[32]))
      return pca_loadings, fa_loadings


  def check_scores_data_tipi():
    scoresdb = pd.DataFrame(merged_person_result_v3)
    user_hol6 = ['survey_holl6_Artistic', 'survey_holl6_Conventional', 'survey_holl6_Enterprising', 'survey_holl6_Investigative', 'survey_holl6_Realistic', 'survey_holl6_Social']
    user_tipi = ['Extraversion_tipi', 'Conscientiousness_tipi', 'Emotional_Stability_tipi', 'Agreeableness_tipi', 'Openness_tipi']
    scores = ['agreeableness', 'artistic', 'conscientiousness', 'conventional', 'enterprising', 'extraversion', 'investigative', 'neuroticism', 'openness', 'realistic', 'social']
    circle_vars = ['overlap',  'overlap_distance', 'rank', 'size', 'standard_distance']
    circle_vars = ['rank', 'size', 'standard_distance']
    circles_pos = ['circles_test_1_Sociable_', 'circles_test_1_Self-Disciplined_', 'circles_test_3_Calm_', 'circles_test_1_Cooperative_', 'circles_test_1_Curious_']
    circles_neg = ['circles_test_3_Self-Reflective_', 'circles_test_3_Disorganized_', 'circles_test_1_Anxious_', 'circles_test_3_Independent_', 'circles_test_3_Traditional_']

    circles_1 = ['circles_test_1_Sociable_', 'circles_test_1_Self-Disciplined_', 'circles_test_1_Anxious_', 'circles_test_1_Cooperative_', 'circles_test_1_Curious_']
    circles_3 = ['circles_test_3_Self-Reflective_', 'circles_test_3_Disorganized_', 'circles_test_3_Calm_', 'circles_test_3_Independent_', 'circles_test_3_Traditional_']
    big5_cols = ['extraversion', 'conscientiousness', 'neuroticism', 'agreeableness', 'openness']

    user_tipi_pos = ['survey_tipi_extraverted', 'survey_tipi_dependable', 'survey_tipi_calm', 'survey_tipi_sympathetic', 'survey_tipi_open']
    user_tipi_neg = ['survey_tipi_reserved', 'survey_tipi_disorganized', 'survey_tipi_anxious', 'survey_tipi_critical', 'survey_tipi_conventional']

    for b in big5_cols:
      vars()[b + '_cir_cols'] = []
      vars()[b + '_cir_cols_p'] = []
      vars()[b + '_cir_cols_n'] = []

    big5_col = []
    std_dist = []
    for n, i in enumerate(circles_pos):
      for c in circle_vars:
        vars()[big5_cols[n] + '_cir_cols_p'].append(circles_pos[n] + c)
        vars()[big5_cols[n] + '_cir_cols_n'].append(circles_neg[n] + c)
        vars()[big5_cols[n] + '_cir_cols'].append(circles_1[n] + c + '_sc')
        vars()[big5_cols[n] + '_cir_cols'].append(circles_3[n] + c + '_sc')
        big5_col.append(circles_pos[n] + c)
        big5_col.append(circles_neg[n] + c)
        std_dist.append(circles_pos[n] + 'standard_distance')
        std_dist.append(circles_neg[n] + 'standard_distance')
    for b in big5_cols:
      print vars()[b + '_cir_cols'], "\n"
    big5_tipi_cols = ['Extraversion_tipi', 'Conscientiousness_tipi', 'Emotional_Stability_tipi', 'Agreeableness_tipi', 'Openness_tipi']

    for a in circle_vars:
      print a
      _vars_to_scale1 = []
      _vars_to_scale3 = []
      for n, i in enumerate(circles_pos):
        _vars_to_scale1.append(circles_1[n] + a)
        _vars_to_scale3.append(circles_3[n] + a)
      scoresdb['min_val'+a] = pd.DataFrame(scoresdb, columns = _vars_to_scale1).min(axis=1)
      scoresdb['max_val'+a] = pd.DataFrame(scoresdb, columns = _vars_to_scale3).max(axis=1)

      for v in _vars_to_scale1:
        try:
          scoresdb[v + '_sc'] = (scoresdb[v] - scoresdb['min_val'+a]) / ( scoresdb['max_val'+a] - scoresdb['min_val'+a] )
        except:
          print v
          for k, v in scoresdb.iteritems():
            print k,
      for v in _vars_to_scale3:
        try:
          scoresdb[v + '_sc'] = (scoresdb[v] - scoresdb['min_val'+a]) / ( scoresdb['max_val'+a] - scoresdb['min_val'+a] )
        except:
          print v

    for n, i in enumerate(big5_cols):
      _df_orig = pd.DataFrame(scoresdb, columns = vars()[i + '_cir_cols'] + [user_tipi[n]] + [user_tipi_pos[n]] + [user_tipi_neg[n]], dtype = 'float64')
      print _df_orig.describe()
      print "\n\n"
      _df_tipi = _df_orig[_df_orig[user_tipi[n]].notnull()]
      print _df_tipi
      _df = _df_orig.dropna()
      _df2 = pd.DataFrame(_df, columns = vars()[i + '_cir_cols'] + [user_tipi_pos[n]] + [user_tipi_neg[n]])
      print _df2.corr()
      _df2 = sm.add_constant(_df2, prepend=False)
      results = sm.OLS(_df[user_tipi[n]], _df2).fit()
      print results.summary(), "\n"
      _df2_ft = np.array(_df2, dtype='float64')
      _df_a = np.array(_df[user_tipi[n]])
      B,SE,PVAL,INMODEL,STATS,NEXTSTEP,HISTORY = stepwisefit(_df2_ft, _df_a)
      print INMODEL, "\n", PVAL
      print "\n\n"
    scores_check = pd.DataFrame(merged_person_result_v3, columns = [user_tipi + user_hol6 + scores], dtype = 'float64')

    std_dist = []
    for n, i in enumerate(circles_pos):
      std_dist.append(circles_pos[n] + 'standard_distance')
      std_dist.append(circles_neg[n] + 'standard_distance')

    big5_scoredb_stddist = pd.DataFrame(merged_person_result_v5, columns = std_dist)
    big5_scoredb_stddist['min_standard_distance'] = pd.DataFrame(big5_scoredb_stddist, columns = std_dist).min(axis=1)
    big5_scoredb_stddist['min_standard_distance'] = pd.DataFrame(big5_scoredb_stddist, columns = std_dist).max(axis=1)


  def stepwise_regression():
    circle_vars = ['overlap',  'overlap_distance', 'rank', 'size', 'standard_distance']
    circle_vars = ['overlap',  'rank', 'size', 'standard_distance']
    circles_pos = ['circles_test_1_Sociable_', 'circles_test_1_Self-Disciplined_', 'circles_test_3_Calm_', 'circles_test_1_Curious_', 'circles_test_1_Cooperative_']
    circles_neg = ['circles_test_3_Self-Reflective_', 'circles_test_3_Disorganized_', 'circles_test_1_Anxious_', 'circles_test_3_Adamant_', 'circles_test_3_Independent_']
    big5_cols = ['agreeableness',  'conscientiousness',  'extraversion', 'neuroticism',  'openness']

    for b in big5_cols:
      vars()[b + '_cir_cols'] = []

    big5_col = []
    for n, i in enumerate(circles_pos):
      for c in circle_vars:
        vars()[big5_cols[n] + '_cir_cols'].append(circles_pos[n] + c)
        vars()[big5_cols[n] + '_cir_cols'].append(circles_neg[n] + c)
        big5_col.append(circles_pos[n] + c)
        big5_col.append(circles_neg[n] + c)

    for b in big5_cols:
      print vars()[b + '_cir_cols'], "\n"
    big5_tipi_cols = ['Agreeableness_tipi', 'Conscientiousness_tipi', 'Emotional_Stability_tipi', 'Extraversion_tipi', 'Openness_tipi']
    for n, i in enumerate(big5_cols):
      _df = merged_person_result_prof_descrip_users[merged_person_result_prof_descrip_users[big5_tipi_cols[n]].notnull()]
      _df2 = pd.DataFrame(_df, columns = vars()[i + '_cir_cols'])
      _df2 = sm.add_constant(_df2, prepend=False)
      results = sm.OLS(_df[big5_tipi_cols[n]], _df2).fit()
      print results.summary(), "\n"

      #stepwise fit
      _df2_ft = np.array(_df2, dtype='float64')
      _df_a = np.array(_df[big5_tipi_cols[n]])
      B,SE,PVAL,INMODEL,STATS,NEXTSTEP,HISTORY = stepwisefit(_df2_ft, _df_a)
      print INMODEL, "\n", PVAL
      print "\n\n"


  def scores():
    check_scores = pd.DataFrame(merged_person_result_prof_descrip)

    for i in ['agreeableness',  'conscientiousness',  'extraversion', 'neuroticism',  'openness']:
      check_scores['circles_' + i] = (check_scores['circles_test_1_' + i] + check_scores['circles_test_3_' + i]) / 2.0

    for i in ['artistic', 'conventional', 'enterprising', 'investigative',  'realistic',  'social']:
      check_scores['circles_' + i] = check_scores['circles_test_4_' + i]

    cols_to_keep = ['index', 'game_id', 'big5_score']
    for x in ['agreeableness',  'conscientiousness',  'extraversion', 'neuroticism',  'openness']:
      cols_to_keep.append(x)
      cols_to_keep.append('circles_' + x)
    scoredb = pd.DataFrame(check_scores, columns = cols_to_keep)
    scoredb.to_csv('check_scores.csv')

    big5_holland6_columns_all = ['circles_test_1_agreeableness',  'circles_test_1_conscientiousness',  'circles_test_1_extraversion', 'circles_test_1_neuroticism',  'circles_test_1_openness',
      'circles_test_3_agreeableness',  'circles_test_3_conscientiousness',  'circles_test_3_extraversion', 'circles_test_3_neuroticism',  'circles_test_3_openness',
      'circles_test_4_artistic', 'circles_test_4_conventional', 'circles_test_4_enterprising', 'circles_test_4_investigative',  'circles_test_4_realistic',  'circles_test_4_social',
      'image_rank_2_agreeableness',  'image_rank_2_conscientiousness',  'image_rank_2_extraversion', 'image_rank_2_neuroticism',  'image_rank_2_openness',
      'image_rank_5_agreeableness',  'image_rank_5_conscientiousness',  'image_rank_5_extraversion', 'image_rank_5_neuroticism',  'image_rank_5_openness',
      'image_rank_6_agreeableness',  'image_rank_6_conscientiousness',  'image_rank_6_extraversion', 'image_rank_6_neuroticism',  'image_rank_6_openness',
      'agreeableness', 'artistic', 'conscientiousness', 'conventional', 'enterprising', 'extraversion', 'investigative', 'neuroticism', 'openness', 'realistic', 'social',
      'Agreeableness_tipi', 'Conscientiousness_tipi', 'Emotional_Stability_tipi',  'Extraversion_tipi', 'Openness_tipi',
      'big5_score', 'holland6_score', 'big5_dimension', 'holland6_dimension', 'big5_low', 'big5_high',
      'personality_id', 'user_id', 'game_id']

    scoredb = pd.DataFrame(merged_person_result_prof_descrip_users, columns = big5_holland6_columns_all)

    ### holland 6
    holland6_columns_all = ['circles_test_4_artistic', 'circles_test_4_conventional', 'circles_test_4_enterprising', 'circles_test_4_investigative',  'circles_test_4_realistic',  'circles_test_4_social',
      'artistic', 'conventional', 'enterprising', 'investigative', 'realistic', 'social', 'holland6_score', 'holland6_dimension', 'personality_id', 'user_id', 'game_id']

    holland_scoredb = pd.DataFrame(merged_person_result_prof_descrip_users, columns = holland6_columns_all)
    holland_scoredb.to_csv("holland_scoredb_v3.csv")

    ### big 5
    big5_columns_all = ['circles_test_1_agreeableness',  'circles_test_1_conscientiousness',  'circles_test_1_extraversion', 'circles_test_1_neuroticism',  'circles_test_1_openness',
      'circles_test_3_agreeableness',  'circles_test_3_conscientiousness',  'circles_test_3_extraversion', 'circles_test_3_neuroticism',  'circles_test_3_openness',
      'agreeableness', 'conscientiousness', 'extraversion', 'neuroticism', 'openness',
      'Agreeableness_tipi', 'Conscientiousness_tipi', 'Emotional_Stability_tipi',  'Extraversion_tipi', 'Openness_tipi',
      'big5_score', 'big5_dimension', 'big5_low', 'big5_high', 'personality_id', 'user_id', 'game_id']

    big5_scoredb = pd.DataFrame(merged_person_result_prof_descrip_users, columns = big5_columns_all)

    for i in ['agreeableness',  'conscientiousness',  'extraversion', 'neuroticism',  'openness']:
      big5_scoredb['circles_' + i] = (big5_scoredb['circles_test_1_' + i] + big5_scoredb['circles_test_3_' + i]) / 2.0

    big5_scoredb.to_csv("big5_scoredb_v1.csv")


    ### compare TIPI
    big5_scoredb = pd.DataFrame(merged_person_result_prof_descrip_users)
    for i in ['agreeableness',  'conscientiousness',  'extraversion', 'neuroticism',  'openness']:
      big5_scoredb['circles_test_' + i + '_avg_zscore'] = (big5_scoredb['circles_test_1_' + i + '_zscore'] + big5_scoredb['circles_test_3_' + i + '_zscore']) / 2.0
      merged_person_result_prof_descrip_users['circles_test_' + i + '_avg_zscore'] = (merged_person_result_prof_descrip_users['circles_test_1_' + i + '_zscore'] + merged_person_result_prof_descrip_users['circles_test_3_' + i + '_zscore']) / 2.0

    big5_tipi_cols = ['Agreeableness_tipi', 'Conscientiousness_tipi', 'Emotional_Stability_tipi', 'Extraversion_tipi', 'Openness_tipi', 'circles_test_agreeableness_avg_zscore', 'circles_test_conscientiousness_avg_zscore', 'circles_test_neuroticism_avg_zscore', 'circles_test_extraversion_avg_zscore', 'circles_test_openness_avg_zscore']
    big5_tipi_df = pd.DataFrame(big5_scoredb, columns = big5_tipi_cols, dtype = 'float64')

    merged_person_result_prof_descrip_users.to_csv('alpha_results_v1_070113.csv')


    ### compare TIPI std dist
    big5_std_distance_cols = ['circles_test_1_Sociable_standard_distance', 'circles_test_1_Self-Disciplined_standard_distance', 'circles_test_3_Calm_standard_distance', 'circles_test_1_Curious_standard_distance', 'circles_test_1_Cooperative_standard_distance', \
      'circles_test_3_Self-Reflective_standard_distance', 'circles_test_3_Disorganized_standard_distance', 'circles_test_1_Anxious_standard_distance', 'circles_test_3_Adamant_standard_distance', 'circles_test_3_Independent_standard_distance']
    tipi_cols = ['Agreeableness_tipi', 'Conscientiousness_tipi', 'Emotional_Stability_tipi', 'Extraversion_tipi', 'Openness_tipi']
    big5_scoredb_stddist = pd.DataFrame(merged_person_result_prof_descrip_users,columns = big5_std_distance_cols + tipi_cols)
    big5_scoredb_stddist['max_standard_distance'] = pd.DataFrame(big5_scoredb_stddist, columns = big5_std_distance_cols).apply(lambda x: x.max(), axis=1)

    postrait1 = ['circles_test_1_Sociable_standard_distance', 'circles_test_1_Self-Disciplined_standard_distance', 'circles_test_3_Calm_standard_distance', 'circles_test_1_Curious_standard_distance', 'circles_test_1_Cooperative_standard_distance']
    negtrait2 = ['circles_test_3_Self-Reflective_standard_distance', 'circles_test_3_Disorganized_standard_distance', 'circles_test_1_Anxious_standard_distance', 'circles_test_3_Adamant_standard_distance', 'circles_test_3_Independent_standard_distance']

    for n, i in enumerate(['agreeableness',  'conscientiousness',  'extraversion', 'emotional_stability',  'openness']):
      big5_scoredb_stddist[i + '_stddist'] = (big5_scoredb_stddist[postrait1[n]] + ( big5_scoredb_stddist['max_standard_distance'] - big5_scoredb_stddist[negtrait2[n]])) / 2

    big5_tipi_cols = ['Agreeableness_tipi', 'Conscientiousness_tipi', 'Emotional_Stability_tipi', 'Extraversion_tipi', 'Openness_tipi', 'agreeableness_stddist',  'conscientiousness_stddist',  'extraversion_stddist', 'emotional_stability_stddist',  'openness_stddist']
    big5_scoredb_stddist_tipi = pd.DataFrame(big5_scoredb_stddist, columns = big5_tipi_cols, dtype = 'float64')


    ### compare TIPI with size
    big5_size_cols = ['circles_test_1_Sociable_size', 'circles_test_1_Self-Disciplined_size', 'circles_test_3_Calm_size', 'circles_test_1_Curious_size', 'circles_test_1_Cooperative_size', \
      'circles_test_3_Self-Reflective_size', 'circles_test_3_Disorganized_size', 'circles_test_1_Anxious_size', 'circles_test_3_Adamant_size', 'circles_test_3_Independent_size']
    tipi_cols = ['Agreeableness_tipi', 'Conscientiousness_tipi', 'Emotional_Stability_tipi', 'Extraversion_tipi', 'Openness_tipi']
    big5_scoredb_size = pd.DataFrame(merged_person_result_prof_descrip_users,columns = big5_size_cols + tipi_cols)
    # big5_scoredb_size['max_size'] = pd.DataFrame(big5_scoredb_size, columns = big5_size_cols).apply(lambda x: x.max(), axis=1)

    postrait1 = ['circles_test_1_Sociable_size', 'circles_test_1_Self-Disciplined_size', 'circles_test_3_Calm_size', 'circles_test_1_Curious_size', 'circles_test_1_Cooperative_size']
    negtrait2 = ['circles_test_3_Self-Reflective_size', 'circles_test_3_Disorganized_size', 'circles_test_1_Anxious_size', 'circles_test_3_Adamant_size', 'circles_test_3_Independent_size']

    for n, i in enumerate(['agreeableness',  'conscientiousness',  'extraversion', 'emotional_stability',  'openness']):
      big5_scoredb_size[i + '_size'] = (big5_scoredb_size[postrait1[n]] + ( 5 - big5_scoredb_size[negtrait2[n]])) / 2

    big5_tipi_cols = ['Agreeableness_tipi', 'Conscientiousness_tipi', 'Emotional_Stability_tipi', 'Extraversion_tipi', 'Openness_tipi', 'agreeableness_size',  'conscientiousness_size',  'extraversion_size', 'emotional_stability_size',  'openness_size']
    big5_scoredb_size_tipi = pd.DataFrame(big5_scoredb_size, columns = big5_tipi_cols, dtype = 'float64')




    ### compare TIPI with size * stddist
    big5_size_cols = ['circles_test_1_Sociable_size', 'circles_test_1_Self-Disciplined_size', 'circles_test_3_Calm_size', 'circles_test_1_Curious_size', 'circles_test_1_Cooperative_size', \
      'circles_test_3_Self-Reflective_size', 'circles_test_3_Disorganized_size', 'circles_test_1_Anxious_size', 'circles_test_3_Adamant_size', 'circles_test_3_Independent_size']
    big5_std_distance_cols = ['circles_test_1_Sociable_standard_distance', 'circles_test_1_Self-Disciplined_standard_distance', 'circles_test_3_Calm_standard_distance', 'circles_test_1_Curious_standard_distance', 'circles_test_1_Cooperative_standard_distance', \
      'circles_test_3_Self-Reflective_standard_distance', 'circles_test_3_Disorganized_standard_distance', 'circles_test_1_Anxious_standard_distance', 'circles_test_3_Adamant_standard_distance', 'circles_test_3_Independent_standard_distance']
    tipi_cols = ['Agreeableness_tipi', 'Conscientiousness_tipi', 'Emotional_Stability_tipi', 'Extraversion_tipi', 'Openness_tipi']
    big5_scoredb_size_stddst = pd.DataFrame(merged_person_result_prof_descrip_users,columns = big5_size_cols + tipi_cols + big5_std_distance_cols)
    big5_scoredb_size_stddst['max_standard_distance'] = pd.DataFrame(big5_scoredb_size_stddst, columns = big5_std_distance_cols).apply(lambda x: x.max(), axis=1)

    postrait1 = ['circles_test_1_Sociable_', 'circles_test_1_Self-Disciplined_', 'circles_test_3_Calm_', 'circles_test_1_Curious_', 'circles_test_1_Cooperative_']
    negtrait2 = ['circles_test_3_Self-Reflective_', 'circles_test_3_Disorganized_', 'circles_test_1_Anxious_', 'circles_test_3_Adamant_', 'circles_test_3_Independent_']

    for n, i in enumerate(['agreeableness',  'conscientiousness',  'extraversion', 'emotional_stability',  'openness']):
      big5_scoredb_size_stddst[i + '_positive_a'] = (big5_scoredb_size_stddst[postrait1[n] + 'size'] + 1)
      big5_scoredb_size_stddst[i + '_positive_b'] = ( big5_scoredb_size_stddst['max_standard_distance'] - big5_scoredb_size_stddst[postrait1[n] + 'standard_distance'])

      big5_scoredb_size_stddst[i + '_negative_a'] = (5 - big5_scoredb_size_stddst[negtrait2[n] + 'size'])
      big5_scoredb_size_stddst[i + '_negative_b'] = big5_scoredb_size_stddst[negtrait2[n] + 'standard_distance']

      # big5_scoredb_size_stddst[i + '_size'] = (big5_scoredb_size_stddst[postrait1[n + 'size']] + ( 5 - big5_scoredb_size_stddst[negtrait2[n + 'size']])) / 2
      # big5_scoredb_size_stddst[i + '_stddist'] = (big5_scoredb_size_stddst[postrait1[n + 'standard_distance']] + ( big5_scoredb_size_stddst['max_standard_distance'] - big5_scoredb_size_stddst[negtrait2[n + 'standard_distance']])) / 2

      big5_scoredb_size_stddst[i + '_size_stddst'] = (big5_scoredb_size_stddst[i + '_positive_a'] * big5_scoredb_size_stddst[i + '_positive_b']) +  (big5_scoredb_size_stddst[i + '_negative_a'] * big5_scoredb_size_stddst[i + '_negative_b']) / 2.0


    big5_tipi_cols = ['Agreeableness_tipi', 'Conscientiousness_tipi', 'Emotional_Stability_tipi', 'Extraversion_tipi', 'Openness_tipi', 'agreeableness_size_stddst',  'conscientiousness_size_stddst',  'extraversion_size_stddst', 'emotional_stability_size_stddst',  'openness_size_stddst']
    big5_scoredb_size_stddst_tipi = pd.DataFrame(big5_scoredb_size_stddst, columns = big5_tipi_cols, dtype = 'float64')


    posneg = ['agreeableness_positive_a',  'conscientiousness_positive_a',  'extraversion_positive_a', 'emotional_stability_positive_a',  'openness_positive_a', 'agreeableness_negative_a',  'conscientiousness_negative_a',  'extraversion_negative_a', 'emotional_stability_negative_a',  'openness_negative_a' \
      'agreeableness_positive_b',  'conscientiousness_positive_b',  'extraversion_positive_b', 'emotional_stability_positive_b',  'openness_positive_b', 'agreeableness_negative_b',  'conscientiousness_negative_b',  'extraversion_negative_b', 'emotional_stability_negative_b',  'openness_negative_b']
    big5_scoredb_posneg = pd.DataFrame(big5_scoredb_size_stddst, columns = posneg, dtype = 'float64')

    ###
    # old_tipi
    size_and_dist_vars = ['circles_test_1_Sociable_size', 'circles_test_1_Self-Disciplined_size', 'circles_test_3_Calm_size', 'circles_test_1_Curious_size', 'circles_test_1_Cooperative_size', \
      'circles_test_1_Sociable_standard_distance', 'circles_test_1_Self-Disciplined_standard_distance', 'circles_test_3_Calm_standard_distance', 'circles_test_1_Curious_standard_distance', 'circles_test_1_Cooperative_standard_distance', \
      'Answer.Extraverted', 'Answer.Sympathetic', 'Answer.Dependable', 'Answer.Calm', 'Answer.experiences']

    size_and_dist_vars_df = pd.DataFrame(merged_person_result_prof_descrip_users, columns = size_and_dist_vars, dtype = 'float64')


  def pull_big5_holland6_min():
    b5h6_verify_columns = ['artistic', 'conventional', 'enterprising', 'investigative', 'realistic', 'social', 'openness', 'agreeableness', 'conscientiousness', 'extraversion', 'neuroticism', 'big5_score', 'holland6_score']
    merged_person_result_prof_descrip_users_v2 = pd.DataFrame(merged_person_result_prof_descrip_users, columns = b5h6_verify_columns)
    merged_person_result_prof_descrip_users_v2['big5_min'] = pd.DataFrame(merged_person_result_prof_descrip_users_v2, columns=['openness', 'agreeableness', 'conscientiousness', 'extraversion', 'neuroticism']).apply(lambda x: x.min(), axis=1).abs()
    merged_person_result_prof_descrip_users_v2['holland6_min'] = pd.DataFrame(merged_person_result_prof_descrip_users_v2, columns=['artistic', 'conventional', 'enterprising', 'investigative', 'realistic', 'social']).apply(lambda x: x.min(), axis=1).abs()

    big5_tipi_cols = ['Agreeableness_tipi', 'Conscientiousness_tipi', 'Emotional_Stability_tipi', 'Extraversion_tipi', 'Openness_tipi', 'agreeableness', 'conscientiousness', 'neuroticism', 'extraversion', 'openness']
    big5_tipi_df = pd.DataFrame(merged_person_result_prof_descrip_users, columns = big5_tipi_cols)

    return merged_person_result_prof_descrip_users_v2



# db_games_by_user = pull_data_site(date_to_pull = '2013-09-22', output_file_name = 'output_092313', mturk_batch_name = 'none')
pull_data_site(date_to_pull = sys.argv[1], output_file_name = sys.argv[2], mturk_batch_name = 'none')



