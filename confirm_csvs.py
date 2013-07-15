import pandas as pd


def check_csv_data():
  csv_name = 'circles_orig'
  vars()[csv_name] = pd.read_csv("%s.csv" % csv_name)

  csv_name = 'circles'
  vars()[csv_name] = pd.read_csv("%s.csv" % csv_name)

  for x in circles.columns:
    if circles[x].all() != circles_orig[x].all():
      print "ERROR", circles[x], circles_orig[x]

  for x in circles.columns:
    try:
      for k,v in circles[x].iteritems():
        if v != circles_orig[x][k]:
          print "NOT match"
          print k, " ", v, "!=", circles_orig[x][k]
          print "\n"
    except:
      print "MISSING", x



  csv_name = 'element_weights_orig'
  vars()[csv_name] = pd.read_csv("%s.csv" % csv_name)

  csv_name = 'element_weights'
  vars()[csv_name] = pd.read_csv("%s.csv" % csv_name)

  for x in element_weights_orig.columns:
    try:
      for k,v in element_weights_orig[x].iteritems():
        if v != element_weights[x][k]:
          print "NOT match"
          print k, " ", v, "!=", element_weights[x][k]
          print "\n"
      # if element_weights[x].all() != element_weights_orig[x].all():
      #   print "ERROR", circles[x], circles_orig[x]
    except:
      print "MISSING", x



def clean_csv(profile_file):
  import pandas as pd

  profdsp = pd.read_csv("%s.csv" % profile_file, header=None)

  for k, v in profdsp.iterrows():
    for a, b in v.iteritems():
      try:
        for y,z in enumerate(b):
          try:
            z.decode('UTF-8', 'strict')
          except UnicodeDecodeError:
            #print "\nERROR!", i, " ", a
            #print b[y], " ", z, " ", y, " =\n",b
            #print b[y-3], b[y-2], b[y-1]
            #print y, " ", b[y-3:y+3], " =\n", b
            print y, " ", b[y-2:y+2]
            print b
      except TypeError:
        print b

  profdsp[0] = profdsp[0].str.lower().str.rstrip().str.lstrip().str.replace(' ', '_')
  profdsp[1] = profdsp[1].str.lower()
  profdsp[3] = profdsp[3].str.rstrip().str.lstrip()


  profdsp.to_csv('%s_v2.csv' % profile_file, header=False, index=False)

clean_csv('profile_descriptions_new_v1')





# import csv
# mypath = customerbulk.objects.get(pk=1).fileup.path

# o = open(mypath,'rU')
# o.seek(0)
# mydata = csv.reader(o)

# f = 'profile_descriptions_utf8.csv'

# #cr = csv.reader(open(f,"rb"))
# o = open(f,'rU')
# o.seek(0)
# cr = csv.reader(o)

# for row in cr:
#     #print '\t'.join(row)
#     # print row
#     # print "\n"
#     try:
#       '\t'.join(row).decode('UTF-8', 'strict')
#     except UnicodeDecodeError:
#       # line contains non-utf8 character
#       print "\n\n\n\n\n\n\n\nERROR!"

# # cr.close()



# for mode in ['r', 'rU', 'rb']:
#   with open(f, mode) as f:
#     try:
#       print '%02s: %r' %(mode, f.read())
#     except:
#       print "type"




# import pandas as pd
# profile_file = 'profile_descriptions_new'
# profdsp = pd.read_csv("%s.csv" % profile_file, header=None)

# for k, v in profdsp.iterrows():
#   for a, b in v.iteritems():
#     try:
#       for i in b:
#         try:
#           i.decode('UTF-8', 'strict')
#         except UnicodeDecodeError:
#           print "\nERROR!", i, " ", a
#           print b[6]
#     except TypeError:
#       pass
#     #print b, "\n\n"


# import re
# p = r"^[\w'-]+$"
# if re.search(p, profdsp[4][1]):
#     pass
# else:
#     print 'error'


# for i in profdsp[4][0]:
#   try:
#     i.decode('UTF-8', 'strict')
#   except UnicodeDecodeError:
#     print "issue", i