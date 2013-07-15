import psycopg2
import psycopg2.extras
import csv
import re
import json
import math
import datetime

#from pandas import DataFrame, Series
import pandas as pd
import numpy as np
# import mdp

import pandas.io.sql as pd_sql

#reader = csv.reader(open("Batch_results_1.csv", "rb"))
#batch1 = list(reader)


def write_to_db(frame, dbname_read, make_db=0):
  cursor.execute("createdb test")
  dbname_read2 = 'dbname=%s' % dbname_read
  dbconn = psycopg2.connect(dbname_read2)
  cursor = dbconn.cursor()


  if make_db == 1:
    # frame.columns
    cursor.execute("createdb test")

    # frame = all_her_mt2
    table_columns_create = []
    for i in frame.columns:
      if frame[i].dtype == 'object':
          dtype_sql = 'varchar(255)'
      elif frame[i].dtype == 'int64':
          dtype_sql = 'integer'
      elif frame[i].dtype == 'float64':
          dtype_sql = 'real'

      table_columns_create.append(i + ' ' + dtype_sql)
      # table_columns_create.append(i)

    print table_columns_create, "\n\n"
    str_cols = ','.join(table_columns_create)

    # str_cols.replace('-', '_')
    # print str_cols

    cursor.execute("""CREATE TABLE cars(id INT PRIMARY KEY, name VARCHAR(20), price INT)""")

    query = """
    CREATE TABLE test
    ( %s
    );""" % str_cols

    cursor.execute(query)
    #cursor.close()

  table_name = 'test'

  # frame is your dataframe
  wildcards = ','.join(['%s'] * len(frame.columns))

  data = [tuple(x) for x in frame.values]

  cursor.executemany("INSERT INTO %s VALUES(%s)" % (table_name, wildcards), data)

  cursor.close()
  dbconn.close()
  # dbconn.commit()

  # pd_sql.write_frame(frame, dbname_read2, dbconn)


  # data = [('Atlanta', 'Georgia', 1.25, 6),
  #         ('Tallahassee', 'Florida', 2.6, 3),
  #         ('Sacramento', 'California', 1.7, 5)]
  # stmt = "INSERT INTO test VALUES(?, ?, ?, ?)"

  # con.executemany(stmt, data)
  # con.commit()

  # # to read from database
  # import pandas.io.sql as sql
  # sql.read_frame('select * from test', con)



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
  assmt_ids = pd.concat(pieces_flat, ignore_index=True)

  print "\n\n", assmt_pieces, "\n\n"
  print "\n\n", assmt_ids, "\n\n"

  return assmt_ids


def read_personalities(dbname_read, ids_to_find, db_name, db_field, db_name_columns):
  dbname_read2 = 'dbname=%s' % dbname_read
  dbconn = psycopg2.connect(dbname_read2)
  cursor = dbconn.cursor()
  cursor.execute('SELECT * FROM %s where %s = ANY (%s) ORDER BY id;' % (db_name, db_field, "%s"), (ids_to_find,))

  pieces = []
  pieces_flat = []

  for row in cursor.fetchall():
    # user_db = pd.DataFrame([row])
    # pieces_flat.append(user_db)

    try:
      column7 = json.loads(row[7])
      for key, resultsrow in enumerate(column7):
        read_results = json.loads(resultsrow)

        frame = pd.DataFrame([read_results])
        frame['assessment_id'] = row[0]
        frame['user_id'] = row[4]
        pieces.append(frame)
    except:
      pass

  cursor.close()
  dbconn.close()

  #db = pd.concat(pieces, ignore_index=True)
  if get_flat == 1:
    db_flat = pd.concat(pieces_flat, ignore_index=True)
  else:
    db_flat = []

  return db_flat



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


def parse_scores_db(_results):
  results = pd.DataFrame(_results, columns=['id', 'game_id', 'score hstore', 'calculations', 'user_id', 'time_played', 'type']).rename(columns={'id': 'results_id'})
  bgpdall = []
  for name, group in results.groupby('user_id'):
    bg = {}
    for k, v in  group.iterrows():
      # print "\n"
      # print v["type"]
      # print v['score hstore']['adjust_by']
      adj_by = float(v['score hstore']['adjust_by'])

      aggresult = json.loads(v['calculations'])
      # print aggresult['dimension_values']
      for i in aggresult['dimension_values']:
        # print i, aggresult['dimension_values'][i]
        # print i, (aggresult['dimension_values'][i] / 10.0) - adj_by
        bg[i] = (aggresult['dimension_values'][i] / 10.0) - adj_by

    bgpd = pd.DataFrame([bg])
    bgpd['user_id'] = name
    bgpdall.append(bgpd)
  big5_holland6_df = pd.concat(bgpdall, ignore_index=True)
  #print big5_holland6_df
  return big5_holland6_df


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

  for user_name, user_group in _games_db.groupby('id'):
    for row in user_group['event_log']:
      try:
        column7 = json.loads(row)
        for key, resultsrow in enumerate(column7):
          frame = pd.DataFrame([resultsrow])
          frame['game_id'] = user_name
          # frame['user_id'] = user_group['user_id']
          # frame['calling_ip'] = user_group['calling_ip']
          # frame['assessment_id'] = user_name
          pieces.append(frame)
      except:
          pass

  event_log_db = pd.concat(pieces, ignore_index=True)

  return event_log_db


# a = agg_results_db[agg_results_db['game_id']==49]

# for rec in agg_results_db:
#     fnuts = pd.DataFrame(rec['nutrients'])
#     fnuts['id'] = rec['id']
#     nutrients.append(fnuts)

# nutrients = pd.concat(nutrients, ignore_index=True)



def score_tipi(mturk_db):
  mturk_db['Extraversion_tipi'] = (mturk_db['survey_tipi_extraverted'] + ( 8 - mturk_db['survey_tipi_reserved'])) / 2
  mturk_db['Agreeableness_tipi'] = (mturk_db['survey_tipi_sympathetic'] + ( 8 - mturk_db['survey_tipi_critical'])) / 2
  mturk_db['Conscientiousness_tipi'] = (mturk_db['survey_tipi_dependable'] + ( 8 - mturk_db['survey_tipi_disorganized'])) / 2
  mturk_db['Emotional_Stability_tipi'] = (mturk_db['survey_tipi_calm'] + ( 8 - mturk_db['survey_tipi_anxious'])) / 2
  mturk_db['Openness_tipi'] = (mturk_db['survey_tipi_open'] + ( 8 - mturk_db['survey_tipi_conventional'])) / 2
  return mturk_db

# can delete
def parse_heroku_data(cursor, dbconn, series_ids, get_flat = 0):
  pieces = []
  pieces_flat = []

  for row in cursor.fetchall():
    # if row[0] >= 340:
    if row[0] in series_ids:
      try:
        column7 = json.loads(row[7])
        if get_flat == 1:
          frame_flat = pd.DataFrame([column7])
          frame_flat['assessment_id'] = row[0]
          frame_flat['user_id'] = row[4]
          pieces_flat.append(frame_flat)

        # print frame_flat

        for key, resultsrow in enumerate(column7):
          read_results = json.loads(resultsrow)
          # print read_results
          # print key, '+=+', resultsrow
          # print "__\n"

          frame = pd.DataFrame([read_results])
          frame['assessment_id'] = row[0]
          frame['user_id'] = row[4]
          pieces.append(frame)

      except:
        pass

  cursor.close()
  db = pd.concat(pieces, ignore_index=True)
  if get_flat == 1:
    db_flat = pd.concat(pieces_flat, ignore_index=True)
  else:
    db_flat = []

  return db_flat, db


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


def parse_event_log(reaction_time, user_id_group = 'user_id'):
  rt_df_pieces = []

  score = 1
  if score == 1:
    # circle data
    circles_data = pd.read_csv("/Users/mirajsanghvi/code/python/new_assessment_061813/circles.csv")
    circles_data.index = circles_data['name_pair']
    # image data
    image_data = pd.read_csv("/Users/mirajsanghvi/code/python/new_assessment_061813/image_codings_v2.csv")
    image_data.index = image_data['imagename']
    # element data
    element_data = pd.read_csv("/Users/mirajsanghvi/code/python/new_assessment_061813/element_weights_v2.csv")
    element_data.index = element_data['name']


  big5_buckets = ['extraversion', 'conscientiousness', 'neuroticism', 'openness', 'agreeableness']
  # reaction_time_user_dict = {}

  for name, group in reaction_time.groupby(user_id_group):
    # print group, "\n"
    # reaction_time_user_dict[name] = {}

    # min_time_start = group['record_time'].min()
    # dt =  datetime.datetime.fromtimestamp(min_time_start/1000.00).strftime('%Y-%m-%d %H:%M:%S')
    total_time = ((group['record_time'].max() - group['record_time'].min())/1000.00)

    game_group_dict = {}
    game_group_dict[name] = { user_id_group: name, 'total_time': total_time }

    for name2, group2 in group.groupby('stage'):
      # print group2, "\n"
      # ct_sequence_dict = {}
      varname = str(group2['module'].iloc[0])

      vars()[varname + "_" + str(name2) + "_time"] = (group2['record_time'].max() - group2['record_time'].min())/1000.00
      num_moves = group2['event_desc'].count() - 2

      game_group_dict[name].update({
        varname + "_" + str(name2) + '_total_time': vars()[varname + "_" + str(name2) + "_time"],
        varname + "_" + str(name2) + '_num_moves': num_moves
      })

      if varname == 'image_rank':
        ir_sequence_dict = {}
        all_elements = []

        for name3, group3 in group2.iterrows():
          ir_sequence_dict[group3['rank']] = group3['image_no']

        try:
          for rank in range(0, 5):
            game_group_dict[name].update({
              varname + "_" + str(name2) + '_rank_' + str(rank): ir_sequence_dict[rank]
            })

            # work with scoring
            pic_image_data = image_data.ix[ir_sequence_dict[rank]]
            pic_image_data2 = pic_image_data[pic_image_data == 1].index

            rev_rank = 5 - rank
            ab = pd.DataFrame(index=pic_image_data2, columns=[str(rev_rank)])
            ab = ab.fillna(rev_rank)
            all_elements.append(ab.T)

          all_elements_df = pd.concat(all_elements)

          # parse out all the vars
          for znum in all_elements_df.index:
            #print all_elements_df.ix[znum]
            for a_ele, b_ele in all_elements_df.ix[znum].iteritems():
              game_group_dict[name].update({
                varname + "_" + str(name2) + '_r' + str(5 - int(znum)) + '_' + a_ele: b_ele
              })

          # print all_elements_df.sum(axis=0), "\n"

          sum_elements = all_elements_df.sum(axis=0)

          for b5 in big5_buckets:
            vars()['weighted_zscore_' + b5] = 0
            vars()['weighted_zscore_' + b5 + '_count'] = 0

          for key, value in sum_elements.iteritems():
            zscore = 0
            if element_data[key]['standard_deviation'] and element_data[key]['standard_deviation'] != 0:
              zscore = (value - element_data[key]['mean']) / element_data[key]['standard_deviation']

            # if np.absolute(zscore) > 4:
            #   print name, name2, " key: ", key, "val: ", value, " mean: ", element_data[key]['mean'], " std ", element_data[key]['standard_deviation'], "z ", zscore, "\n"

            for b5 in big5_buckets:
              if element_data[key]['weight_' + b5] != 0:
                vars()['weighted_zscore_' + b5] += (zscore * element_data[key]['weight_' + b5])
                vars()['weighted_zscore_' + b5 + '_count'] += 1

          for b5 in big5_buckets:
            _weighted_avg = 0
            if vars()['weighted_zscore_' + b5 + '_count'] != 0:
              _weighted_avg = vars()['weighted_zscore_' + b5] / vars()['weighted_zscore_' + b5 + '_count']

            game_group_dict[name].update({
              varname + "_" + str(name2) + "_" + b5: _weighted_avg
            })

        except:
          pass

      elif varname == 'circles_test':
        stage_distance = []
        traits = []
        traits_2 = []

        for name3, group3 in group2.iterrows():

          if group3['event_desc'] == 'test_completed' and group3['module'] == 'circles_test':
            self_top = group3['self_coord']['top']
            self_left = group3['self_coord']['left']
            self_radius = group3['self_coord']['size'] / 2.0

            # for circle vars
            for circle_vals in group3['circles']:
              top = circle_vals['top']
              left = circle_vals['left']
              radius = circle_vals['width'] / 2.0
              trait1 = circle_vals['trait1']
              trait2 = circle_vals['trait2']
              size = circle_vals['size']

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
                varname + "_" + str(name2) + "_" + trait1 + '_standard_distance': num_self_radius_distance
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
          circle_vars_to_get_mean_std_z = ['size', 'rank', 'overlap']
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

      elif varname == 'reaction_time':
        rt_sequence_dict = {}

        for name3, group3 in group2.iterrows():

          if group3['event_desc'] == 'test_started':
            red_times = {}
            color_sequence = group3['color_sequence']
            simp_comp_test = group3['sequence_type']
            # print color_sequence, "here"

            prev_one = ''
            for n, seq in enumerate(color_sequence):
              #print color_sequence[n], "===", seq
              one, two = color_sequence[n].split(':')
              if one == 'red':
                if simp_comp_test == 'simple':
                  red_times[n] = int(two)
                elif simp_comp_test == 'complex' and prev_one == 'yellow':
                  red_times[n] = int(two)
              prev_one = one

          if group3['sequence_no'] in rt_sequence_dict:
            try:
              if rt_sequence_dict[group3['sequence_no']][group3['event_desc']] > group3['record_time']:
                rt_sequence_dict[group3['sequence_no']].update({group3['event_desc']: group3['record_time']})
            except:
              rt_sequence_dict[group3['sequence_no']].update({group3['event_desc']: group3['record_time']})
          else:
            rt_sequence_dict[group3['sequence_no']] = {group3['event_desc']: group3['record_time']}

        correct_event = []
        correct_event_less_thres = []
        wrong_event = []
        wrong_event_less_thres = []
        missed_event = []

        correct_event_prev_color = []
        wrong_event_prev_color = []
        missed_event_prev_color = []
        correct_event_prev_time = []
        wrong_event_prev_time = []
        missed_event_prev_time = []

        #for k, v in sequence_dict.iteritems():
        for k in sorted(rt_sequence_dict):
          try:
            time_clicked = (rt_sequence_dict[k]['correct_circle_clicked'] - rt_sequence_dict[k]['circle_shown'])/1000.00
            if time_clicked >= .200:
              correct_event.append(time_clicked)
              if k > 0:
                pre_one, pre_two = color_sequence[int(k) - 1].split(':')
                correct_event_prev_color.append(pre_one)
                correct_event_prev_time.append(int(pre_two)/1000.00)
              else:
                correct_event_prev_color.append('first')
                correct_event_prev_time.append(float('Nan'))
            else:
              correct_event_less_thres.append(time_clicked)
          except:
            pass

          try:
            time_clicked = (rt_sequence_dict[k]['wrong_circle_clicked'] - rt_sequence_dict[k]['circle_shown'])/1000.00
            if time_clicked >= .200:
              wrong_event.append(time_clicked)
              if k > 0:
                pre_one, pre_two = color_sequence[int(k) - 1].split(':')
                wrong_event_prev_color.append(pre_one)
                wrong_event_prev_time.append(int(pre_two)/1000.00)
              else:
                wrong_event_prev_color.append('first')
                wrong_event_prev_time.append(float('Nan'))
            else:
              wrong_event_less_thres.append(time_clicked)
          except:
            pass

          # checked for missed events
          if k in red_times:
            try:
              if rt_sequence_dict[k]['correct_circle_clicked']:
                pass
            except:
              missed_time = red_times[k]/1000.00
              missed_event.append(missed_time)
              if k > 0:
                # print color_sequence[int(k) - 1]
                pre_one, pre_two = color_sequence[int(k) - 1].split(':')
                missed_event_prev_color.append(pre_one)
                missed_event_prev_time.append(int(pre_two)/1000.00)
              else:
                missed_event_prev_color.append('first')
                missed_event_prev_time.append(float('Nan'))

        # vars()[varname + "_time"] = (vars()[varname + "_end_time"] - vars()[varname + "_start_time"])/1000.00
        avg_correct_event = np.mean(correct_event)
        avg_wrong_event = np.mean(wrong_event)
        len_correct_event = len(correct_event)
        len_wrong_event = len(wrong_event)
        len_missed_event = len(missed_event)

        # add in something for missing events:
        process_time_wrong = np.sum(wrong_event)
        process_time_correct = np.sum(correct_event)
        process_time_correct_wrong = process_time_correct + process_time_wrong
        process_time_correct_wrong_reqboth = process_time_correct_wrong

        if len_correct_event == 0:
          process_time_correct = float('NaN')
        if len_wrong_event == 0:
          process_time_wrong = float('NaN')
        if len_correct_event == 0 and len_wrong_event == 0:
          process_time_correct_wrong = float('NaN')
        if len_correct_event == 0 or len_wrong_event == 0:
          process_time_correct_wrong_reqboth = float('NaN')

        # processing_time_total_minus_cw = total_time
        try:
          min_correct_event = np.amin(correct_event)
          max_correct_event = np.amax(correct_event)
        except:
          min_correct_event = max_correct_event = float('NaN')
        try:
          min_wrong_event = np.amin(wrong_event)
          max_wrong_event = np.amax(wrong_event)
        except:
          min_wrong_event = max_wrong_event = float('NaN')
        try:
          min_missed_event = np.amin(missed_event)
          max_missed_event = np.amax(missed_event)
        except:
          min_missed_event = max_missed_event = float('NaN')

        ## threshold
        len_correct_event_less_thres = len(correct_event_less_thres)
        len_wrong_event_less_thres = len(wrong_event_less_thres)

        vars_rt_interest = [correct_event, wrong_event, missed_event, correct_event_less_thres, correct_event_prev_color, wrong_event_prev_color, missed_event_prev_color, correct_event_prev_time, wrong_event_prev_time, missed_event_prev_time]

        for n in range(10):
          globals()['correct_event_' + str(n)] = float('NaN')
          globals()['wrong_event_' + str(n)] = float('NaN')
          globals()['missed_event_' + str(n)] = 0.00
          globals()['correct_event_less_thres_' + str(n)] = float('NaN')

          globals()['correct_event_prev_color_' + str(n)] = ''
          globals()['wrong_event_prev_color_' + str(n)] = ''
          globals()['missed_event_prev_color_' + str(n)] = ''
          globals()['correct_event_prev_time_' + str(n)] = float('NaN')
          globals()['wrong_event_prev_time_' + str(n)] = float('NaN')
          globals()['missed_event_prev_time_' + str(n)] = float('NaN')

        # for vars_rt in vars_rt_interest:
        #   for n_event, l_event in enumerate( globals()[vars_rt] ):
        #     globals()[str(vars_rt) + '_' + str(n_event)] = l_event

        for n_event, l_event in enumerate(correct_event):
          globals()['correct_event_' + str(n_event)] = l_event

        for n_event, l_event in enumerate(wrong_event):
          globals()['wrong_event_' + str(n_event)] = l_event

        for n_event, l_event in enumerate(missed_event):
          globals()['missed_event_' + str(n_event)] = l_event

        for n_event, l_event in enumerate(correct_event_less_thres):
          globals()['correct_event_less_thres_' + str(n_event)] = l_event

        for n_event, l_event in enumerate(correct_event_prev_color):
          globals()['correct_event_prev_color_' + str(n_event)] = l_event

        for n_event, l_event in enumerate(wrong_event_prev_color):
          globals()['wrong_event_prev_color_' + str(n_event)] = l_event

        for n_event, l_event in enumerate(missed_event_prev_color):
          globals()['missed_event_prev_color_' + str(n_event)] = l_event

        for n_event, l_event in enumerate(correct_event_prev_time):
          globals()['correct_event_prev_time_' + str(n_event)] = l_event

        for n_event, l_event in enumerate(wrong_event_prev_time):
          globals()['wrong_event_prev_time_' + str(n_event)] = l_event

        for n_event, l_event in enumerate(missed_event_prev_time):
          globals()['missed_event_prev_time_' + str(n_event)] = l_event

        # getting stand dev for values
        _3_events_first3 = _3_events_low3 = avg_correct_event_min3 = float('NaN')
        if len_correct_event >= 3:
          _3_events_first3 = correct_event[0] + correct_event[1] + correct_event[2]
          sorted_correct_event = sorted(correct_event)
          #print sorted_correct_event, "++\n"
          _3_events_low3 = sorted_correct_event[0] + sorted_correct_event[1] + sorted_correct_event[2]
          #print _3_events_first3, _3_events_low3, "--\n"
          avg_correct_event_min3 = avg_correct_event

        max_wrg_events = len_wrong_event
        max_range = len_correct_event + len_missed_event + len_correct_event_less_thres

        game_group_dict[name].update({
          varname + '_' + simp_comp_test + '_avg_correct_time': avg_correct_event, varname + '_' + simp_comp_test + '_avg_wrong_time': avg_wrong_event,
          varname + '_' + simp_comp_test + '_correct_count': len_correct_event, varname + '_' + simp_comp_test + '_wrong_count': len_wrong_event,
          varname + '_' + simp_comp_test + '_correct_count_less_thres': len_correct_event_less_thres, varname + '_' + simp_comp_test + '_wrong_count_less_thres': len_wrong_event_less_thres,
          varname + '_' + simp_comp_test + '_correct_event_min': min_correct_event, varname + '_' + simp_comp_test + '_correct_event_max': max_correct_event,
          varname + '_' + simp_comp_test + '_wrong_event_min': min_wrong_event, varname + '_' + simp_comp_test + '_wrong_event_max': max_wrong_event,
          varname + '_' + simp_comp_test + '_missed_event_min': min_missed_event, varname + '_' + simp_comp_test + '_missed_event_max': max_missed_event,
          varname + '_' + simp_comp_test + '_process_time_corr_wrg': process_time_correct_wrong, varname + '_' + simp_comp_test + '_process_time_corr_wrg_reqboth': process_time_correct_wrong_reqboth,
          varname + '_' + simp_comp_test + '_process_time_correct': process_time_correct, varname + '_' + simp_comp_test + '_process_time_wrong': process_time_wrong,
          varname + '_' + simp_comp_test + '_3_events_sum_first3': _3_events_first3, varname + '_' + simp_comp_test + '_3_events_sum_low3': _3_events_low3, varname + '_' + simp_comp_test + '_avg_correct_events_3plus': avg_correct_event_min3
          })

        for corr_events in range(0, max_range):
          game_group_dict[name].update({
            varname + '_' + simp_comp_test + '_correct_event_' + str(corr_events + 1): globals()['correct_event_' + str(corr_events)],
            varname + '_' + simp_comp_test + '_correct_event_prev_color_' + str(corr_events + 1): globals()['correct_event_prev_color_' + str(corr_events)],
            varname + '_' + simp_comp_test + '_correct_event_prev_time_' + str(corr_events + 1): globals()['correct_event_prev_time_' + str(corr_events)],
            varname + '_' + simp_comp_test + '_missed_event_' + str(corr_events + 1): globals()['missed_event_' + str(corr_events)],
            varname + '_' + simp_comp_test + '_missed_event_prev_color_' + str(corr_events + 1): globals()['missed_event_prev_color_' + str(corr_events)],
            varname + '_' + simp_comp_test + '_missed_event_prev_time_' + str(corr_events + 1): globals()['missed_event_prev_time_' + str(corr_events)] })

        for wrg_events in range(0, max_wrg_events):
          game_group_dict[name].update({
            varname + '_' + simp_comp_test + '_wrong_event_' + str(wrg_events + 1): globals()['wrong_event_' + str(wrg_events)],
            varname + '_' + simp_comp_test + '_wrong_event_prev_color_' + str(wrg_events + 1): globals()['wrong_event_prev_color_' + str(wrg_events)],
            varname + '_' + simp_comp_test + '_wrong_event_prev_time_' + str(wrg_events + 1): globals()['wrong_event_prev_time_' + str(wrg_events)] })

      elif varname == 'emotions_circles':
        stage_distance = []
        traits = []

        for name3, group3 in group2.iterrows():
          if group3['event_desc'] == 'test_completed':
            self_top = group3['self_coord']['top']
            self_left = group3['self_coord']['left']
            self_radius = group3['self_coord']['size'] / 2.0

            # for circle vars
            for circle_vals in group3['circles']:
              top = circle_vals['top']
              left = circle_vals['left']
              radius = circle_vals['width'] / 2.0
              trait1 = circle_vals['trait1']
              # size = circle_vals['size']

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
              # traits_2.append(trait2)

              # new standard distance
              num_self_radius_distance = result_distance / self_radius

              # write to db
              game_group_dict[name].update({
                varname + "_" + str(name2) + "_" + trait1 + '_distance': result_distance,
                varname + "_" + str(name2) + "_" + trait1 + '_overlap': result_overlap,
                varname + "_" + str(name2) + "_" + trait1 + '_overlap_distance': result_overlap_distance,
                varname + "_" + str(name2) + "_" + trait1 + '_standard_distance': num_self_radius_distance,
                varname + "_" + str(name2) + "_" + trait1 + '_stage': name2
              })

        # rank the circles
        for n, i in enumerate(traits):
          result_distance = game_group_dict[name][varname + "_" + str(name2) + "_" + i + '_distance']

          rank = stage_distance.index(result_distance)
          game_group_dict[name].update({
            varname + "_" + str(name2) + "_" + i + '_rank': rank
          })

      elif varname == 'survey':
        for i in group2[group2['questions'].notnull()]['questions']:
          for qs2k in i:
            # game_group_dict[name].update({
            #   varname + "_" + str(name2) + "_" + qs2k['question_id']: qs2k['answer']
            # })
            game_group_dict[name].update({
              varname + "_" + qs2k['question_id']: qs2k['answer']
            })

    # get rid of errors in data
    rt_df = pd.DataFrame(game_group_dict)
    rt_df_pieces.append(rt_df.T)

  reaction_time_dfall = pd.concat(rt_df_pieces, ignore_index=True)

  return reaction_time_dfall


def work_with_db(reaction_time_dfall):
    mturk_emo_labels_time = {'a': 'relief', 'b':'anger', 'c':'pride', 'd':'triumph', 'e':'contentment', 'f':'surprise', 'g':'happiness',
        'h': 'interest', 'i': 'amused', 'j': 'desire_food', 'k': 'awe', 'l': 'pain', 'm': 'disgust', 'n': 'boredom', 'o': 'embarrassment',
        'p': 'sympathy', 'q': 'sadness', 'r': 'desire_sex', 's': 'shame', 't': 'confused', 'u': 'coyness', 'w': 'fear' }

    _q1_columns = []
    _q2_columns = []
    _qtime_columns = []

    try:
        for k, v in mturk_emo_labels_time.iteritems():
            reaction_time_dfall["e_" + v + '_time'] = (reaction_time_dfall[v + '_end_time'] - reaction_time_dfall[v + '_start_time'])/1000.00
            # reaction_time_dfall["e_" + v + '_time_old'] = (reaction_time_dfall[v + '_end_time'] - reaction_time_dfall[v + '_start_time_old'])/1000.00
            delete_columns_times = [v + '_end_time', v + '_start_time']
            reaction_time_dfall = reaction_time_dfall.drop(delete_columns_times, axis=1)
            _q1_columns.extend(['e_' + v + '_q1'])
            _q2_columns.extend(['e_' + v + '_q2'])
            _qtime_columns.extend(['e_' + v + '_time'])
            reaction_time_dfall['e_' + v + '_q1'] = reaction_time_dfall['e_' + v + '_q1'].astype(float)
            reaction_time_dfall['e_' + v + '_q2'] = reaction_time_dfall['e_' + v + '_q2'].astype(float)
            # reaction_time_dfall['e_' + v + '_distance']

        # getting data per user
        reaction_time_dfall['e_a_min_q1'] = pd.DataFrame(reaction_time_dfall, columns = _q1_columns).min(axis=1)
        reaction_time_dfall['e_a_max_q1'] = pd.DataFrame(reaction_time_dfall, columns = _q1_columns).max(axis=1)
        reaction_time_dfall['e_a_mean_q1'] = pd.DataFrame(reaction_time_dfall, columns = _q1_columns).mean(axis=1)

        reaction_time_dfall['e_a_min_q2'] = pd.DataFrame(reaction_time_dfall, columns = _q2_columns).min(axis=1)
        reaction_time_dfall['e_a_max_q2'] = pd.DataFrame(reaction_time_dfall, columns = _q2_columns).max(axis=1)
        reaction_time_dfall['e_a_mean_q2'] = pd.DataFrame(reaction_time_dfall, columns = _q2_columns).mean(axis=1)

        reaction_time_dfall['e_a_min_time'] = pd.DataFrame(reaction_time_dfall, columns = _qtime_columns).min(axis=1)
        reaction_time_dfall['e_a_max_time'] = pd.DataFrame(reaction_time_dfall, columns = _qtime_columns).max(axis=1)
        reaction_time_dfall['e_a_mean_time'] = pd.DataFrame(reaction_time_dfall, columns = _qtime_columns).mean(axis=1)
    except:
        pass

    # _col_1 = []

    # for k, v in mturk_emo_labels.iteritems():
    #     _col_1.extend(['e_' + v + '_distance'])
    #     _col_1.extend(['e_' + v + '_stage'])
    #     _col_1.extend(['e_' + v + '_rank'])
    # q1 = pd.DataFrame(reaction_time_dfall2, columns=_col_1)

    # # for k, v in mturk_emo_labels_time.iteritems():
    # #     reaction_time_dfall['e_' + v + '_rank'] = ""

    # def x_position_def(x):
    #     x_position = x['e_' + str(x['e_' + v + '_stage']) + '_stage_distances'].index(x['e_' + v + '_distance'])
    #     return x_position
    # for k, v in mturk_emo_labels_time.iteritems():
    #     reaction_time_dfall['e_' + v + '_rank'] = ""
            # x_position = reaction_time_dfall['e_' + str(reaction_time_dfall['e_' + v + '_stage']) + '_stage_distances'].index(reaction_time_dfall['e_' + v + '_distance'])
            # print x_position
        # if reaction_time_dfall['e_' + v + '_stage'] == 0:
        #     print reaction_time_dfall['e_' + '0' + '_stage_distances'], "\n\n"
        #     print reaction_time_dfall['e_' + v + '_distance'], " :: ", x_position


    # get difference of total
    reaction_time_dfall['complex_minus_simple_time'] = reaction_time_dfall['complex_total_time'] - reaction_time_dfall['simple_total_time']

    reaction_time_dfall['total_process_time'] = reaction_time_dfall['complex_process_time_corr_wrg'] + reaction_time_dfall['simple_process_time_corr_wrg']

    reaction_time_dfall['total_time_mns_process_time'] = reaction_time_dfall['total_time'] - reaction_time_dfall['total_process_time']

    # mean
    vars_to_get_mean_std_z = ['simple_3_events_sum_first3', 'simple_3_events_sum_low3', 'simple_avg_correct_events_3plus', 'complex_3_events_sum_first3',
        'complex_3_events_sum_low3', 'complex_avg_correct_events_3plus']

    for var in vars_to_get_mean_std_z:
        reaction_time_dfall[var + '_mean'] = reaction_time_dfall[var].mean()
        reaction_time_dfall[var + '_std'] = reaction_time_dfall[var].std()
        reaction_time_dfall[var + '_zscore'] = ((reaction_time_dfall[var] - reaction_time_dfall[var + '_mean'])/reaction_time_dfall[var + '_std'])


    # to add in rank for distances
    for k, v in mturk_emo_labels_time.iteritems():
        reaction_time_dfall['e_' + v + '_rank'] = ""

    reaction_time_dfall2 = reaction_time_dfall.apply(x_position_def, axis = 1)

    # drop distance columns
    delete_columns = ['e_0_stage_distances', 'e_1_stage_distances', 'e_2_stage_distances', 'e_3_stage_distances']
    reaction_time_dfall3 = reaction_time_dfall2.drop(delete_columns, axis=1)

    # reaction_time_dfall_sorted = reaction_time_dfall.sort_index(axis = 1)
    # return reaction_time_dfall_sorted
    return reaction_time_dfall3


def do_all_with_latest():
    heroku_db_name = 'heroku_052913'

    ## work with batches for overall data
    batch_names = ["Batch_1109544_clicks1", "Batch_1110868_clicks2", "Batch_1119007_batch_results", "Batch_1125593_batch_results", "Batch_1132018_batch_results","Batch_1137632_batch_results_simple", "Batch_1137636_batch_results_complex", "Batch_1151465_batch_results"]
    batch_assmt_id = [5, 5, 5, 5, 15, 14, 13, 16]


    columns_tot_des = ['simple_correct_event_min', 'simple_correct_event_max', 'simple_avg_correct_time', 'complex_correct_event_min', 'complex_correct_event_max', 'complex_avg_correct_time']

    # mturk_emo_labels = {'a': 'relief', 'b':'anger', 'c':'pride', 'd':'triumph', 'e':'contentment', 'f':'surprise', 'g':'happiness',
    #     'h': 'interest', 'i': 'amused', 'j': 'desire_food', 'k': 'awe', 'l': 'pain', 'm': 'disgust', 'n': 'boredom', 'o': 'embarrassment',
    #     'p': 'sympathy', 'q': 'sadness', 'r': 'desire_sex', 's': 'shame', 't': 'confused', 'u': 'coyness', 'w': 'fear' }
    # for k, v in mturk_emo_labels.iteritems():
    #     columns_tot_des.extend(['e_' + v + '_q1'])
    #     columns_tot_des.extend(['e_' + v + '_q2'])


    for i, batches in enumerate(batch_names):
        globals()['batch' + str(i)] = get_population_avgs(batches, batch_assmt_id[i], heroku_db_name)
        globals()['batch_a' + str(i)] = pd.DataFrame(globals()['batch' + str(i)], columns=columns_tot_des)

    batch_01 = batch_a0.merge(batch_a1, how="outer")
    batch_23 = batch_a2.merge(batch_a3, how="outer")
    batch_45 = batch_a4.merge(batch_a5, how="outer")
    batch_67 = batch_a6.merge(batch_a7, how="outer")

    #

    batch0123 = batch_01.merge(batch_23, how="outer")
    batch4567 = batch_45.merge(batch_67, how="outer")

    batch0123456 = batch0123.merge(batch4567, how="outer")


    print batch0123456.describe()
    print "\n\n", batch0123456.mean(), "\n", batch0123456.std(), "\n\n"
    # raise SystemExit


    out_csv_batch_name = "mturk_052913_22emotions"
    definition_id = 16
    mturk_batch_name = "Batch_1151465_batch_results"


    # out_csv_batch_name = "mturk_052313_22emotions_complex_v2"
    # definition_id = 13
    # mturk_batch_name = "Batch_1137636_batch_results_complex"


    mturk_data = read_mturk_data(mturk_batch_name)
    list_mturk = np.sort(mturk_data['assessment_id']).tolist()

    cursor, dbconn = read_heroku_db(heroku_db_name, definition_id)

    reaction_time_flat, reaction_time = parse_heroku_data(cursor, dbconn, list_mturk)

    dbconn.close()

    reaction_time_dfall = parse_heroku_db(reaction_time)

    # reaction_time_dfall.to_csv('test_batch3.csv')

    reaction_time_dfall_v2 = work_with_db(reaction_time_dfall)

    # get zscores
    # columns_tot_des = ['simple_correct_event_min', 'simple_correct_event_max', 'simple_avg_correct_time']
    for var in columns_tot_des:
        reaction_time_dfall_v2[var + '_mean'] = batch0123456[var].mean()
        reaction_time_dfall_v2[var + '_std'] = batch0123456[var].std()
        try:
            reaction_time_dfall_v2[var + '_zscore'] = ((reaction_time_dfall_v2[var] - reaction_time_dfall_v2[var + '_mean'])/reaction_time_dfall_v2[var + '_std'])
            #print reaction_time_dfall_v2[var].dtype
        except:
            reaction_time_dfall_v2[var] = reaction_time_dfall_v2[var].astype(float)
            reaction_time_dfall_v2[var + '_zscore'] = ((reaction_time_dfall_v2[var] - reaction_time_dfall_v2[var + '_mean'])/reaction_time_dfall_v2[var + '_std'])
            #print "failed", reaction_time_dfall_v2[var],  reaction_time_dfall_v2[var].dtype, "f\n"
            #print reaction_time_dfall_v2[var][0].dtype


    reaction_time_dfall_v2_sorted = reaction_time_dfall_v2.sort_index(axis = 1)

    test3 = pd.merge(reaction_time_dfall_v2_sorted, mturk_data, on='assessment_id')

    test3.to_csv('%s.csv' % out_csv_batch_name)



def reaction_time_multiple(heroku_db_name, mturk_batch_name):
    #heroku_db_name = 'heroku_052313'
    # out_csv_batch_name = "mturk_052313_22emotions_simple_v2"
    # mturk_batch_name = "Batch_1137632_batch_results_simple"

    mturk_data = read_mturk_data(mturk_batch_name)
    list_mturk = np.sort(mturk_data['assessment_id']).tolist()

    work_with_users = 1
    if work_with_users == 1:
        assessment_ids = read_users_db(heroku_db_name, list_mturk)
        assmt_id_username = assessment_ids[[0,1,6]].rename(columns={0: 'user_id', 1:'name', 6: 'email'})

        do_merge_ds = 1
        if do_merge_ds == 1:
            mturk_data['assessment_id_lower'] = mturk_data['assessment_id'].str.lower()

            assmt_id_username['_name_lower'] = assmt_id_username['name'].str.lower()
            _name_merge = pd.merge(assmt_id_username, mturk_data, how='inner', left_on='_name_lower', right_on='assessment_id_lower')

            assmt_id_username['_email_lower'] = assmt_id_username['email'].str.lower()
            _email_merge = pd.merge(assmt_id_username, mturk_data, how='inner', left_on='_email_lower', right_on='assessment_id_lower')

            mturk_data_e_n = pd.concat([_name_merge, _email_merge], ignore_index=True)

            mturk_data_e_n.drop_duplicates(cols='email', inplace=True)

            mturk_data_e_n_v2 = mturk_data_e_n.drop(['_email_lower', '_name_lower'], axis=1)

        list_user_ids = list(assessment_ids[0].T)

        reaction_time_flat, reaction_time = read_heroku_db_user_ids(heroku_db_name, list_user_ids, 1)

        reaction_time_with_email = merge_mturk_data_heroku(assmt_id_username, reaction_time)

        reaction_time_dfall = parse_heroku_db(reaction_time_with_email)

        pieces_delta = []
        for name, group in reaction_time_dfall.groupby('email'):
            group['delta'] = (group['assessment_start_time_dt'] - group['assessment_start_time_dt'].shift()).fillna(0)
            for x in group['delta']:
                if x * 2.77778e-13 >= 2:
                    print name, x
            pieces_delta.append(group)

        reaction_time_dfall_v2 = pd.concat(pieces_delta, ignore_index=True)

        reaction_time_and_mt_all = pd.merge(reaction_time_dfall_v2, mturk_data_e_n_v2, on='email')

        reaction_time_and_mt_all['assessment_start_time_dt'] = reaction_time_and_mt_all['assessment_start_time_dt'].apply(lambda x: x.tz_localize('utc'))

        time_zone_to_code = {
          "Eastern Time Zone": 'EST',
          "Pacific Time Zone": 'PST',
          "Central Time Zone": 'CST',
          "Mountain Time Zone": 'MST',
          "Hawaiian Time Zone": 'HST',
          "Alaskan Time Zone": 'AKST',
          "Indian standard time": 'IST',
          "Central European Time": 'CET',
          "Greenwich time zone": 'GMT' }


        reaction_time_and_mt_all['timezone_code'] = reaction_time_and_mt_all['Answer.timezone'].map(time_zone_to_code)

        def time_zone_change(x):
            try:
                return x['assessment_start_time_dt'].tz_convert(x['timezone_code'])
            except:
                return x['assessment_start_time_dt']

        reaction_time_and_mt_all['assessment_start_time_dt_local'] = reaction_time_and_mt_all.apply(time_zone_change, axis=1)


        #reaction_time_and_mt_all['assessment_start_time_dt_local'] = reaction_time_and_mt_all['assessment_start_time_dt'].apply(lambda x: x.tz_convert('EST'))
        #reaction_time_and_mt_all['assessment_start_time_dt_local'] = reaction_time_and_mt_all['assessment_start_time_dt'].apply(lambda x: x.tz_convert(reaction_time_and_mt_all['timezone_code'][x]))


        ##
        reaction_time_and_mt_all.to_csv('results_RT_2.csv')

        pieces_email_grp = []
        for name, group in reaction_time_and_mt_all.groupby('email'):
            # new_df = group.reset_index()
            # new_df_2 = new_df.unstack().rename(lambda x: '%s-%d' % x)
            # new_df_3 = pd.DataFrame([new_df_2])
            new_df_3 = pd.DataFrame([group.reset_index().unstack().rename(lambda x: '%s-%d' % x)])
            pieces_email_grp.append(new_df_3)

        reaction_time_and_mt_all_v2 = pd.concat(pieces_email_grp, ignore_index=True)

        reaction_time_and_mt_all_v2.to_csv('results_RT_2_flat.csv')

        return reaction_time_and_mt_all_v2d


            #return x[0] + x[1]
            # print x['timezone_code']
            # return x['assessment_start_time_dt'].tz_convert('EST')

            # if x['timezone_code'] == 'GMT':
            #     return x['assessment_start_time_dt'].tz_convert('PST')
            # elif x['timezone_code'] == 'EST':
            #     return x['assessment_start_time_dt'].tz_convert('EST')
            # else:
            #     return "1"

        # reaction_time_and_mt_all.unstack().rename(lambda x: '%s-%d' % x)

        # reaction_time_and_mt_all.groupby('email').reset_index().unstack().rename(lambda x: '%s-%d' % x)

            # print "grp"
            # print group
            # print "new group"
            # print group.unstack().rename(lambda x: '%s-%d' % x)
            # print "end\n\n"


        # for k in enumerate(reaction_time_and_mt_all.ix):
        #     print reaction_time_and_mt_all[k]

        # reaction_time_and_mt_all['assessment_start_time_dt']

        # reaction_time_and_mt_all['assessment_start_time_dt'].tz_convert('EST')

        # s.apply(lambda x: Timestamp(x).tz_localize('UTC'))

        # reaction_time_and_mt_all['assessment_start_time_dt_code'] = reaction_time_and_mt_all['assessment_start_time_dt'].map(time_zone_to_code)

        # reaction_time_and_mt_all['local'] = reaction_time_and_mt_all['assessment_start_time_dt'].tz_localize('UTC')

        # return reaction_time_dfall_v2, mturk_data

        # raise SystemExit

        # datetime

        # # reaction_time_dfall = reaction_time_dfall.sort(['user_id', 'assessment_start_time_dt'])

        # times_trial_ran = {}
        # for name, group in reaction_time_dfall.groupby('user_id'):
        #     # print name, "\n", group['assessment_id'], group['assessment_start_time'], group['assessment_start_time_dt'], "end\n"
        #     # print name, group['assessment_start_time_dt'], "end\n"

        #     times_trial_ran[name] = {}
        #     for x, y in enumerate(group['assessment_start_time_dt']):
        #         # print x, " ", y
        #         times_trial_ran[name].update({str(x): y})

        #     # times_trial_ran[name].update()
        # print times_trial_ran


        # for k, v in times_trial_ran.iteritems():
        #     print v, "++"
        #     prev = 0
        #     for x, y in v.iteritems():
        #         print y
        #         print (prev - y)
        #         prev = y


        # for x in reaction_time_dfall_v2['delta']:
        #     if x * 2.77778e-13 >= 2:
        #         print x

        # for k,v in reaction_time_dfall_v2.iteritems():
        #     print k
        #     for x in reaction_time_dfall_v2['delta']:
        #         if x * 2.77778e-13 >= 2:
        #             print k, x

        # for name, group in reaction_time_dfall.groupby('user_id'):
        #     print name, "nhere", group, "end\n\n"
        #     #reaction_time_dfall[name]['delta'] = (reaction_time_dfall[name]['assessment_start_time_dt'] - reaction_time_dfall[name]['assessment_start_time_dt'].shift()).fillna(0)

    # sexual_to_num = {
    #   "straight": 1,
    #   "gay_lesbian": 2,
    #   "bisexual": 3,
    #   "unsure_other": 4 }


    # reaction_time_dfall['delta'] = mturk_data_v2['Answer.gender'].map(gender_to_num)


    #     # assessment_ids = parse_heroku_data(cursor_users, dbconn_users, list_mturk)



    # # reaction_time_dfall.to_csv('test_batch3.csv')

    # reaction_time_dfall_v2 = work_with_db(reaction_time_dfall)


    # test3 = pd.merge(reaction_time_dfall_v2, mturk_data, on='assessment_id')

    # return test3, reaction_time_dfall_v2, mturk_data, reaction_time


def emotion_and_reaction(heroku_db_name, mturk_batch_name):

    mturk_data = read_mturk_data(mturk_batch_name)

    list_mturk = np.sort(mturk_data['assessment_id']).tolist()

    reaction_time_flat, reaction_time = read_heroku_db_assmt_ids(heroku_db_name, list_mturk, 1)

    reaction_time_dfall = parse_heroku_db(reaction_time)

    # reaction_time_dfall.to_csv('test_batch3.csv')

    reaction_time_dfall_v2 = work_with_db(reaction_time_dfall)

    reaction_time_dfall_v2_sorted = reaction_time_dfall_v2.sort_index(axis = 1)

    test3 = pd.merge(reaction_time_dfall_v2_sorted, mturk_data, on='assessment_id')

    return test3, reaction_time_dfall_v2, mturk_data


def new_assessment_work(heroku_db_name, mturk_batch_name):
  mturk_data = read_mturk_data(mturk_batch_name)
  # mturk_data = score_tipi(mturk_data)

  list_mturk = np.sort(mturk_data['assessment_id']).tolist()

  assessment_ids = read_users_db(heroku_db_name, list_mturk)

  no_mt_results = 0
  if no_mt_results == 1:
    assessment_ids = read_users_db_date(heroku_db_name, '2013-07-01')

  assmt_id_username = assessment_ids[[0,1,7]].rename(columns={0: 'user_id', 1: 'email', 7:'name'})

  not_in_db = mturk_data[mturk_data['assessment_id'].isin(assessment_ids[1]) == False]

  mturk_data['assessment_id_lower'] = mturk_data['assessment_id'].str.lower()
  assmt_id_username['_email_lower'] = assmt_id_username['email'].str.lower()
  mturk_data_e_n_v2 = pd.merge(assmt_id_username, mturk_data, how='inner', left_on='_email_lower', right_on='assessment_id_lower').drop(['_email_lower', 'assessment_id_lower'], axis=1)


  # get personality db
  list_user_ids = list(assessment_ids[0].T)
  personality_columns = ['id', 'profile_description_id', 'user_id', 'game_id', 'big5_score', 'holland6_score', 'big5_dimension', 'holland6_dimension', 'big5_low', 'big5_high', 'created_at', 'updated_at']
  personality_db = read_db(heroku_db_name, list_user_ids, 'personalities', 'user_id', personality_columns)
  personality_db = personality_db.rename(columns={'id': 'personality_id'})
  personality_db_v2 = pd.DataFrame(personality_db, columns=['personality_id', 'profile_description_id', 'user_id', 'game_id', 'big5_dimension', 'holland6_dimension', 'big5_low', 'big5_high'])

  # results
  list_game_id = list(personality_db['game_id'].T)
  results_columns = ['id', 'game_id', 'event_log', 'intermediate_results', 'created_at', 'updated_at', 'aggregate_results', 'score hstore', 'calculations', 'user_id', 'time_played', 'time_calculated', 'analysis_version', 'type']
  results_db = read_db_hstore(heroku_db_name, list_game_id, 'results', 'game_id', results_columns)
  # results_db_v2 = pd.DataFrame(results_db, columns=['id', 'game_id', 'score hstore', 'calculations', 'user_id', 'time_played', 'type']).rename(columns={'id': 'results_id'})
  # results_dba = read_db(heroku_db_name, list_game_id, 'results', 'game_id', results_columns)

  # games
  games_columns = ['id', 'date_taken', 'definition_id', 'user_id', 'stages', 'stage_completed', 'created_at', 'updated_at', 'status', 'calling_ip', 'event_log']
  games_db = read_db(heroku_db_name, list_user_ids, 'games', 'user_id', games_columns)
  games_db_usr_ip = pd.DataFrame(games_db, columns = ["id", "user_id", "calling_ip", "date_taken", "definition_id", "status"]).rename(columns={'id': 'game_id'})

  # profile descriptions
  list_profile_description_ids = list(personality_db['profile_description_id'].unique().T)
  list_profile_description_ids.sort()
  profile_descriptions_columns = ['id', 'name', 'description', 'one_liner', 'bullet_description', 'big5_dimension', 'holland6_dimension', 'code', 'logo_url', 'created_at', 'updated_at', 'display_id']
  profile_descriptions_db = read_db(heroku_db_name, list_profile_description_ids, 'profile_descriptions', 'id', profile_descriptions_columns)
  profile_descriptions_db = profile_descriptions_db.rename(columns={'id': 'profile_description_id'})
  profile_descriptions_db_v2 = pd.DataFrame(profile_descriptions_db, columns=['profile_description_id', 'name'])


  # extra work with dbs
  results_db_v2 = parse_scores_db(results_db)

  games_db_v2 = parse_games_columns(games_db)

  games_db_v3 = parse_event_log(games_db_v2, "game_id")

  games_db_v4 = pd.merge(games_db_usr_ip, games_db_v3, on='game_id')

  games_db_v4_sort = games_db_v4.sort(['user_id', 'game_id', 'definition_id'])


  a_pieces_email_grp = []
  b_pieces_email_grp = []
  c_pieces_email_grp = []
  for name, group in games_db_v4_sort.groupby('definition_id'):
    new_df_3=group.dropna(axis=1,how='all')

    for name2, group2 in new_df_3.groupby("user_id"):
      #new_3 = pd.DataFrame([group2.reset_index().unstack().rename(lambda x: '%s-%d' % x)])
      #print group2.sort(['date_taken'], ascending=False), "\n"
      _new3 = group2.sort(['date_taken'], ascending=False)
      new_3 = _new3.iloc[:1]
      #new_3 = group2.iloc[:1]

      if name == 4:
        a_pieces_email_grp.append(new_3)
      elif name == 6:
        b_pieces_email_grp.append(new_3)
      elif name == 7:
        c_pieces_email_grp.append(new_3)

  a_3 = pd.concat(a_pieces_email_grp, ignore_index=True)
  b_3 = pd.concat(b_pieces_email_grp, ignore_index=True)
  c_3 = pd.concat(c_pieces_email_grp, ignore_index=True)


  ab_3 = pd.merge(a_3, b_3, on='user_id')
  abc_3 = pd.merge(ab_3, c_3, on='user_id')
  abc_3_sorted = abc_3.sort_index(axis = 1)


  merged_person_result = pd.merge(abc_3_sorted, results_db_v3, on='user_id')
  merged_person_result_v2 = pd.merge(merged_person_result, personality_db_v2, on='user_id')

  merged_person_result_v3 = score_tipi(merged_person_result_v2)

  merged_person_result_v3.to_csv('research_results_071313_v3.csv')



  # merge results with agg results
  #results_db_v4 = pd.merge(results_db_v3, merged_person_result, on='game_id')

  # merge profile description

  merged_person_result_prof_descrip = pd.merge(merged_person_result, profile_descriptions_db_v2, on='profile_description_id')

  # merge users
  merged_person_result_prof_descrip_users = pd.merge(merged_person_result_prof_descrip, mturk_data_e_n_v2, on='user_id')


  merged_person_result_prof_descrip_users.to_csv('alpha_results_v1_070113.csv')



  def stepwise_regression():
    circle_vars = ['overlap',  'overlap_distance', 'rank', 'size', 'standard_distance']
    circle_vars = ['overlap',  'rank', 'size', 'standard_distance']
    # circles = ['circles_test_1_agreeableness',  'circles_test_1_conscientiousness',  'circles_test_1_extraversion', 'circles_test_1_neuroticism',  'circles_test_1_openness',
    #   'circles_test_3_agreeableness',  'circles_test_3_conscientiousness',  'circles_test_3_extraversion', 'circles_test_3_neuroticism',  'circles_test_3_openness']
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
        ##
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
      #_df2_ft = np.nan_to_num(_df2_ft)

      _df_a = np.array(_df[big5_tipi_cols[n]])
      # _df_a = np.nan_to_num(_df_a)

      B,SE,PVAL,INMODEL,STATS,NEXTSTEP,HISTORY = stepwisefit(_df2_ft, _df_a)

      print INMODEL, "\n", PVAL
      print "\n\n"

    # stepwisefit(np.array(_df2), np.array(_df['Agreeableness_tipi']))
    # plt.plot(_df['Openness_tipi'], results.fittedvalues, 'r--.');
    # model = sm.GLM(merged_person_result_prof_descrip_users['Agreeableness_tipi'], agreeableness_df, family = sm.families.Binomial())
    # logit = sm.Logit(merged_person_result_prof_descrip_users['Agreeableness_tipi'], agreeableness_df)


  def scores():
    #'circles_test_1_agreeableness',  'circles_test_1_conscientiousness',  'circles_test_1_extraversion', 'circles_test_1_neuroticism',  'circles_test_1_openness',
    #'circles_test_3_agreeableness',  'circles_test_3_conscientiousness',  'circles_test_3_extraversion', 'circles_test_3_neuroticism',  'circles_test_3_openness',
    # 'circles_test_4_artistic', 'circles_test_4_conventional', 'circles_test_4_enterprising', 'circles_test_4_investigative',  'circles_test_4_realistic',  'circles_test_4_social'
    check_scores = pd.DataFrame(merged_person_result_prof_descrip)

    for i in ['agreeableness',  'conscientiousness',  'extraversion', 'neuroticism',  'openness']:
      check_scores['circles_' + i] = (check_scores['circles_test_1_' + i] + check_scores['circles_test_3_' + i]) / 2.0

    for i in ['artistic', 'conventional', 'enterprising', 'investigative',  'realistic',  'social']:
      check_scores['circles_' + i] = check_scores['circles_test_4_' + i]

    cols_to_keep = ['index', 'game_id', 'big5_score']
    #for x in ['agreeableness', 'artistic', 'conscientiousness', 'conventional', 'enterprising', 'extraversion', 'investigative', 'neuroticism', 'openness', 'realistic', 'social']:
    for x in ['agreeableness',  'conscientiousness',  'extraversion', 'neuroticism',  'openness']:
      cols_to_keep.append(x)
      cols_to_keep.append('circles_' + x)
      # cols_to_keep.append('circles_test_1_' + x)
      # cols_to_keep.append('circles_test_3_' + x)
      #if check_scores['circles_' + x] == check_scores[x]:

    scoredb = pd.DataFrame(check_scores, columns = cols_to_keep)
    scoredb.to_csv('check_scores.csv')
    ###


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
    # get min values and check values
    b5h6_verify_columns = ['artistic', 'conventional', 'enterprising', 'investigative', 'realistic', 'social', 'openness', 'agreeableness', 'conscientiousness', 'extraversion', 'neuroticism', 'big5_score', 'holland6_score']
    merged_person_result_prof_descrip_users_v2 = pd.DataFrame(merged_person_result_prof_descrip_users, columns = b5h6_verify_columns)
    merged_person_result_prof_descrip_users_v2['big5_min'] = pd.DataFrame(merged_person_result_prof_descrip_users_v2, columns=['openness', 'agreeableness', 'conscientiousness', 'extraversion', 'neuroticism']).apply(lambda x: x.min(), axis=1).abs()
    merged_person_result_prof_descrip_users_v2['holland6_min'] = pd.DataFrame(merged_person_result_prof_descrip_users_v2, columns=['artistic', 'conventional', 'enterprising', 'investigative', 'realistic', 'social']).apply(lambda x: x.min(), axis=1).abs()

    # reaction_time_and_mt_all['assessment_start_time_dt_local'] = reaction_time_and_mt_all.apply(big5_change, axis=1)
    # big5_df.apply(big5_change, axis=1)

    big5_tipi_cols = ['Agreeableness_tipi', 'Conscientiousness_tipi', 'Emotional_Stability_tipi', 'Extraversion_tipi', 'Openness_tipi', 'agreeableness', 'conscientiousness', 'neuroticism', 'extraversion', 'openness']
    big5_tipi_df = pd.DataFrame(merged_person_result_prof_descrip_users, columns = big5_tipi_cols)

    return merged_person_result_prof_descrip_users_v2

  def big5_change(x):
    # lambda x: (big5_df['big5_min'].abs() + x) * 10
    return (big5_df['big5_min'].abs() + x) * 10
    # try:
    #     return x['assessment_start_time_dt'].tz_convert(x['timezone_code'])
    # except:
    #     return x['assessment_start_time_dt']







  ## work_with_dbs


  # raise SystemExit
  do_this = 0
  if do_this == 1:
    # reaction_time_flat, reaction_time = read_heroku_db_user_ids(heroku_db_name, list_user_ids, 1)

    reaction_time_with_email = merge_mturk_data_heroku(assmt_id_username, reaction_time)

    reaction_time_dfall = parse_heroku_db(reaction_time_with_email)

    pieces_delta = []
    for name, group in reaction_time_dfall.groupby('email'):
      group['delta'] = (group['assessment_start_time_dt'] - group['assessment_start_time_dt'].shift()).fillna(0)
      for x in group['delta']:
        if x * 2.77778e-13 >= 2:
          print name, x
      pieces_delta.append(group)

    reaction_time_dfall_v2 = pd.concat(pieces_delta, ignore_index=True)

    reaction_time_and_mt_all = pd.merge(reaction_time_dfall_v2, mturk_data_e_n_v2, on='email')

    reaction_time_and_mt_all['assessment_start_time_dt'] = reaction_time_and_mt_all['assessment_start_time_dt'].apply(lambda x: x.tz_localize('utc'))

    time_zone_to_code = {
      "Eastern Time Zone": 'EST',
      "Pacific Time Zone": 'PST',
      "Central Time Zone": 'CST',
      "Mountain Time Zone": 'MST',
      "Hawaiian Time Zone": 'HST',
      "Alaskan Time Zone": 'AKST',
      "Indian standard time": 'IST',
      "Central European Time": 'CET',
      "Greenwich time zone": 'GMT' }


    reaction_time_and_mt_all['timezone_code'] = reaction_time_and_mt_all['Answer.timezone'].map(time_zone_to_code)

    def time_zone_change(x):
      try:
        return x['assessment_start_time_dt'].tz_convert(x['timezone_code'])
      except:
        return x['assessment_start_time_dt']

    reaction_time_and_mt_all['assessment_start_time_dt_local'] = reaction_time_and_mt_all.apply(time_zone_change, axis=1)

    reaction_time_and_mt_all.to_csv('results_RT_2.csv')

    pieces_email_grp = []
    for name, group in reaction_time_and_mt_all.groupby('email'):
      # new_df = group.reset_index()
      # new_df_2 = new_df.unstack().rename(lambda x: '%s-%d' % x)
      # new_df_3 = pd.DataFrame([new_df_2])
      new_df_3 = pd.DataFrame([group.reset_index().unstack().rename(lambda x: '%s-%d' % x)])
      pieces_email_grp.append(new_df_3)

    reaction_time_and_mt_all_v2 = pd.concat(pieces_email_grp, ignore_index=True)

    reaction_time_and_mt_all_v2.to_csv('results_RT_2_flat.csv')

    return reaction_time_and_mt_all_v2d




heroku_db_name = 'tide_research_071513'
#mturk_batch_name = 'Batch_1190430_batch_results'
mturk_batch_name = 'Batch_1205503_batch_results'



# print mturk_data.ix[:5, :10]
