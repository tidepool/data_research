##

# install.packages('sem')

image_data = read.csv("~/code/python/new_assessment_all/factors_cfa_r_v2.csv")

library(sem)
image.cov <- cov(na.omit( image_data ))

cfa.model <- specifyModel()
FACTOR1 -> image_rank_ele_shading, factor11
FACTOR1 -> image_rank_ele_pair, factor12
FACTOR1 -> image_rank_ele_nature, factor13
FACTOR1 -> image_rank_ele_man_made, factor14
FACTOR2 -> image_rank_ele_color, factor21
FACTOR3 -> image_rank_ele_movement, factor31
FACTOR3 -> image_rank_ele_texture, factor32
FACTOR3 -> image_rank_ele_animal, factor33
FACTOR4 -> image_rank_ele_negative_space, factor41
FACTOR4 -> image_rank_ele_human, factor42
FACTOR5 -> image_rank_ele_whole, factor51
FACTOR5 -> image_rank_ele_abstraction, factor52
FACTOR1 <-> FACTOR1, NA, 1
FACTOR2 <-> FACTOR2, NA, 1
FACTOR3 <-> FACTOR3, NA, 1
FACTOR4 <-> FACTOR4, NA, 1
FACTOR5 <-> FACTOR5, NA, 1
image_rank_ele_shading <-> image_rank_ele_shading, error1
image_rank_ele_pair <-> image_rank_ele_pair, error2
image_rank_ele_nature <-> image_rank_ele_nature, error3
image_rank_ele_man_made <-> image_rank_ele_man_made, error4
image_rank_ele_color <-> image_rank_ele_color, error5
image_rank_ele_movement <-> image_rank_ele_movement, error6
image_rank_ele_texture <-> image_rank_ele_texture, error7
image_rank_ele_animal <-> image_rank_ele_animal, error8
image_rank_ele_negative_space <-> image_rank_ele_negative_space, error9
image_rank_ele_human <-> image_rank_ele_human, error10
image_rank_ele_whole <-> image_rank_ele_whole, error11
image_rank_ele_abstraction <-> image_rank_ele_abstraction, error12
FACTOR1 <-> FACTOR2, cov1
FACTOR1 <-> FACTOR3, cov2
FACTOR1 <-> FACTOR4, cov3
FACTOR1 <-> FACTOR5, cov4
FACTOR2 <-> FACTOR3, cov5
FACTOR2 <-> FACTOR4, cov6
FACTOR2 <-> FACTOR5, cov7
FACTOR3 <-> FACTOR4, cov8
FACTOR3 <-> FACTOR5, cov9
FACTOR4 <-> FACTOR5, cov10

cfa <- sem( cfa.model, image.cov, nrow(image_data))






##

# install.packages('sem')

image_data = read.csv("~/code/python/new_assessment_all/factors_cfa_r.csv")

library(sem)
image.cov <- cov(na.omit( image_data ))

cfa.model <- specifyModel()
FACTOR1 -> image_rank_ele_shading, factor11
FACTOR1 -> image_rank_ele_pair, factor12
FACTOR1 -> image_rank_ele_nature, factor13
FACTOR1 -> image_rank_ele_man_made, factor14
FACTOR2 -> image_rank_ele_color, factor21
FACTOR2 -> image_rank_ele_achromatic, factor22
FACTOR3 -> image_rank_ele_movement, factor31
FACTOR3 -> image_rank_ele_texture, factor32
FACTOR3 -> image_rank_ele_animal, factor33
FACTOR4 -> image_rank_ele_negative_space, factor41
FACTOR4 -> image_rank_ele_human, factor42
FACTOR4 -> image_rank_ele_animal, factor43
FACTOR5 -> image_rank_ele_whole, factor51
FACTOR5 -> image_rank_ele_abstraction, factor52
FACTOR1 <-> FACTOR1, NA, 1
FACTOR2 <-> FACTOR2, NA, 1
FACTOR3 <-> FACTOR3, NA, 1
FACTOR4 <-> FACTOR4, NA, 1
FACTOR5 <-> FACTOR5, NA, 1
image_rank_ele_shading <-> image_rank_ele_shading, error1
image_rank_ele_pair <-> image_rank_ele_pair, error2
image_rank_ele_nature <-> image_rank_ele_nature, error3
image_rank_ele_man_made <-> image_rank_ele_man_made, error4
image_rank_ele_color <-> image_rank_ele_color, error5
image_rank_ele_achromatic <-> image_rank_ele_achromatic, error6
image_rank_ele_movement <-> image_rank_ele_movement, error7
image_rank_ele_texture <-> image_rank_ele_texture, error8
image_rank_ele_animal <-> image_rank_ele_animal, error9
image_rank_ele_negative_space <-> image_rank_ele_negative_space, error10
image_rank_ele_human <-> image_rank_ele_human, error11
image_rank_ele_whole <-> image_rank_ele_whole, error12
image_rank_ele_abstraction <-> image_rank_ele_abstraction, error13
FACTOR1 <-> FACTOR2, cov1
FACTOR1 <-> FACTOR3, cov2
FACTOR1 <-> FACTOR4, cov3
FACTOR1 <-> FACTOR5, cov4
FACTOR2 <-> FACTOR3, cov5
FACTOR2 <-> FACTOR4, cov6
FACTOR2 <-> FACTOR5, cov7
FACTOR3 <-> FACTOR4, cov8
FACTOR3 <-> FACTOR5, cov9
FACTOR4 <-> FACTOR5, cov10

cfa <- sem( cfa.model, image.cov, nrow(image_data))






image_data_fac5 = read.csv("~/code/python/new_assessment_all/factors_cfa_r_v5.csv")

library(sem)
image.cov5 <- cov(na.omit( image_data_fac5 ))

cfa.model <- specifyModel()
FACTOR1 -> factor1, a1
FACTOR2 -> factor2, a2
FACTOR3 -> factor3, a3
FACTOR4 -> factor4, a4
FACTOR5 -> factor5, a5

FACTOR1 <-> FACTOR1, NA, 1
FACTOR2 <-> FACTOR2, NA, 1
FACTOR3 <-> FACTOR3, NA, 1
FACTOR4 <-> FACTOR4, NA, 1
FACTOR5 <-> FACTOR5, NA, 1

factor1 <-> factor1, error1
factor2 <-> factor2, error2
factor3 <-> factor3, error3
factor4 <-> factor4, error4
factor5 <-> factor5, error5
FACTOR1 <-> FACTOR2, cov1
FACTOR1 <-> FACTOR3, cov2
FACTOR1 <-> FACTOR4, cov3
FACTOR1 <-> FACTOR5, cov4
FACTOR2 <-> FACTOR3, cov5
FACTOR2 <-> FACTOR4, cov6
FACTOR2 <-> FACTOR5, cov7
FACTOR3 <-> FACTOR4, cov8
FACTOR3 <-> FACTOR5, cov9
FACTOR4 <-> FACTOR5, cov10

cfa <- sem( cfa.model, image.cov5, nrow(image_data_fac5))




## emoticons
emoticons.cov5 <- cov(na.omit( emoticons_fac1 ))

cfa.model_emo <- specifyModel()
FACTOR4 -> emotions_circles_0_amused_standard_distance, emo1
FACTOR1 -> emotions_circles_0_anger_standard_distance, emo2
FACTOR2 -> emotions_circles_0_coyness_standard_distance, emo3
FACTOR5 -> emotions_circles_0_desire_sex_standard_distance, emo4
FACTOR3 -> emotions_circles_0_interest_standard_distance, emo5
FACTOR1 -> emotions_circles_0_sadness_standard_distance, emo6
FACTOR2 -> emotions_circles_1_confused_standard_distance, emo7
FACTOR3 -> emotions_circles_1_contentment_standard_distance, emo8
FACTOR2 -> emotions_circles_1_embarrassment_standard_distance, emo9
FACTOR1 -> emotions_circles_1_fear_standard_distance, emo10
FACTOR4 -> emotions_circles_1_pride_standard_distance, emo11
FACTOR5 -> emotions_circles_1_boredom_standard_distance, emo12
FACTOR1 -> emotions_circles_2_sympathy_standard_distance, emo13
FACTOR4 -> emotions_circles_2_relief_standard_distance, emo14
FACTOR4 -> emotions_circles_2_triumph_standard_distance, emo15
FACTOR3 -> emotions_circles_2_awe_standard_distance, emo16
FACTOR1 -> emotions_circles_2_disgust_standard_distance, emo17
FACTOR4 -> emotions_circles_3_desire_food_standard_distance, emo18
FACTOR3 -> emotions_circles_3_happiness_standard_distance, emo19
FACTOR1 -> emotions_circles_3_pain_standard_distance, emo20
FACTOR2 -> emotions_circles_3_shame_standard_distance, emo21
FACTOR1 -> emotions_circles_3_surprise_standard_distance, emo22
FACTOR1 <-> FACTOR1, NA, 1
FACTOR2 <-> FACTOR2, NA, 1
FACTOR3 <-> FACTOR3, NA, 1
FACTOR4 <-> FACTOR4, NA, 1
FACTOR5 <-> FACTOR5, NA, 1
emotions_circles_0_amused_standard_distance <-> emotions_circles_0_amused_standard_distance, error1
emotions_circles_0_anger_standard_distance <-> emotions_circles_0_anger_standard_distance, error2
emotions_circles_0_coyness_standard_distance <-> emotions_circles_0_coyness_standard_distance, error3
emotions_circles_0_desire_sex_standard_distance <-> emotions_circles_0_desire_sex_standard_distance, error4
emotions_circles_0_interest_standard_distance <-> emotions_circles_0_interest_standard_distance, error5
emotions_circles_0_sadness_standard_distance <-> emotions_circles_0_sadness_standard_distance, error6
emotions_circles_1_confused_standard_distance <-> emotions_circles_1_confused_standard_distance, error7
emotions_circles_1_contentment_standard_distance <-> emotions_circles_1_contentment_standard_distance, error8
emotions_circles_1_embarrassment_standard_distance <-> emotions_circles_1_embarrassment_standard_distance, error9
emotions_circles_1_fear_standard_distance <-> emotions_circles_1_fear_standard_distance, error10
emotions_circles_1_pride_standard_distance <-> emotions_circles_1_pride_standard_distance, error11
emotions_circles_1_boredom_standard_distance <-> emotions_circles_1_boredom_standard_distance, error12
emotions_circles_2_sympathy_standard_distance <-> emotions_circles_2_sympathy_standard_distance, error13
emotions_circles_2_relief_standard_distance <-> emotions_circles_2_relief_standard_distance, error14
emotions_circles_2_triumph_standard_distance <-> emotions_circles_2_triumph_standard_distance, error15
emotions_circles_2_awe_standard_distance <-> emotions_circles_2_awe_standard_distance, error16
emotions_circles_2_disgust_standard_distance <-> emotions_circles_2_disgust_standard_distance, error17
emotions_circles_3_desire_food_standard_distance <-> emotions_circles_3_desire_food_standard_distance, error18
emotions_circles_3_happiness_standard_distance <-> emotions_circles_3_happiness_standard_distance, error19
emotions_circles_3_pain_standard_distance <-> emotions_circles_3_pain_standard_distance, error20
emotions_circles_3_shame_standard_distance <-> emotions_circles_3_shame_standard_distance, error21
emotions_circles_3_surprise_standard_distance <-> emotions_circles_3_surprise_standard_distance, error22
FACTOR1 <-> FACTOR2, cov1
FACTOR1 <-> FACTOR3, cov2
FACTOR1 <-> FACTOR4, cov3
FACTOR1 <-> FACTOR5, cov4
FACTOR2 <-> FACTOR3, cov5
FACTOR2 <-> FACTOR4, cov6
FACTOR2 <-> FACTOR5, cov7
FACTOR3 <-> FACTOR4, cov8
FACTOR3 <-> FACTOR5, cov9
FACTOR4 <-> FACTOR5, cov10

cfa_emo <- sem( cfa.model_emo, emoticons.cov5, nrow(emoticons_fac1) )



## old emo circles
emo_circles = read.csv("~/code/python/22_emotions_circles_all_vals/emotion_std_dist_3_batches_factor1.csv")

emo_circles.cov5 <- cov(na.omit( emo_circles ))


cfa.model_emo_circles <- specifyModel()
FACTOR4 -> e_amused_standard_distance, emo1
FACTOR1 -> e_anger_standard_distance, emo2
FACTOR2 -> e_coyness_standard_distance, emo3
FACTOR5 -> e_desire_sex_standard_distance, emo4
FACTOR3 -> e_interest_standard_distance, emo5
FACTOR1 -> e_sadness_standard_distance, emo6
FACTOR2 -> e_confused_standard_distance, emo7
FACTOR3 -> e_contentment_standard_distance, emo8
FACTOR2 -> e_embarrassment_standard_distance, emo9
FACTOR1 -> e_fear_standard_distance, emo10
FACTOR4 -> e_pride_standard_distance, emo11
FACTOR5 -> e_boredom_standard_distance, emo12
FACTOR1 -> e_sympathy_standard_distance, emo13
FACTOR4 -> e_relief_standard_distance, emo14
FACTOR4 -> e_triumph_standard_distance, emo15
FACTOR3 -> e_awe_standard_distance, emo16
FACTOR1 -> e_disgust_standard_distance, emo17
FACTOR4 -> e_desire_food_standard_distance, emo18
FACTOR3 -> e_happiness_standard_distance, emo19
FACTOR1 -> e_pain_standard_distance, emo20
FACTOR2 -> e_shame_standard_distance, emo21
FACTOR1 -> e_surprise_standard_distance, emo22
FACTOR1 <-> FACTOR1, NA, 1
FACTOR2 <-> FACTOR2, NA, 1
FACTOR3 <-> FACTOR3, NA, 1
FACTOR4 <-> FACTOR4, NA, 1
FACTOR5 <-> FACTOR5, NA, 1
e_amused_standard_distance <-> e_amused_standard_distance, error1
e_anger_standard_distance <-> e_anger_standard_distance, error2
e_coyness_standard_distance <-> e_coyness_standard_distance, error3
e_desire_sex_standard_distance <-> e_desire_sex_standard_distance, error4
e_interest_standard_distance <-> e_interest_standard_distance, error5
e_sadness_standard_distance <-> e_sadness_standard_distance, error6
e_confused_standard_distance <-> e_confused_standard_distance, error7
e_contentment_standard_distance <-> e_contentment_standard_distance, error8
e_embarrassment_standard_distance <-> e_embarrassment_standard_distance, error9
e_fear_standard_distance <-> e_fear_standard_distance, error10
e_pride_standard_distance <-> e_pride_standard_distance, error11
e_boredom_standard_distance <-> e_boredom_standard_distance, error12
e_sympathy_standard_distance <-> e_sympathy_standard_distance, error13
e_relief_standard_distance <-> e_relief_standard_distance, error14
e_triumph_standard_distance <-> e_triumph_standard_distance, error15
e_awe_standard_distance <-> e_awe_standard_distance, error16
e_disgust_standard_distance <-> e_disgust_standard_distance, error17
e_desire_food_standard_distance <-> e_desire_food_standard_distance, error18
e_happiness_standard_distance <-> e_happiness_standard_distance, error19
e_pain_standard_distance <-> e_pain_standard_distance, error20
e_shame_standard_distance <-> e_shame_standard_distance, error21
e_surprise_standard_distance <-> e_surprise_standard_distance, error22
FACTOR1 <-> FACTOR2, cov1
FACTOR1 <-> FACTOR3, cov2
FACTOR1 <-> FACTOR4, cov3
FACTOR1 <-> FACTOR5, cov4
FACTOR2 <-> FACTOR3, cov5
FACTOR2 <-> FACTOR4, cov6
FACTOR2 <-> FACTOR5, cov7
FACTOR3 <-> FACTOR4, cov8
FACTOR3 <-> FACTOR5, cov9
FACTOR4 <-> FACTOR5, cov10

cfa_emo_circles <- sem( cfa.model_emo_circles, emo_circles.cov5, nrow(emo_circles) )



df_all_data['factor_1'] = (df_all_data['e_anger_standard_distance'] + df_all_data['e_disgust_standard_distance'] +
  df_all_data['e_fear_standard_distance'] + df_all_data['e_pain_standard_distance'] +
  df_all_data['e_sadness_standard_distance'] + df_all_data['e_surprise_standard_distance'] +
  df_all_data['e_sympathy_standard_distance']) / 7

df_all_data['factor_2'] = (df_all_data['e_confused_standard_distance'] + df_all_data['e_coyness_standard_distance'] +
  df_all_data['e_embarrassment_standard_distance'] + df_all_data['e_shame_standard_distance']) / 4

df_all_data['factor_3'] = (df_all_data['e_contentment_standard_distance'] + df_all_data['e_happiness_standard_distance'] +
  df_all_data['e_interest_standard_distance'] + df_all_data['e_awe_standard_distance']) / 4

df_all_data['factor_4'] = (df_all_data['e_amused_standard_distance'] + df_all_data['e_pride_standard_distance'] +
  df_all_data['e_relief_standard_distance'] + df_all_data['e_triumph_standard_distance'] +
  df_all_data['e_desire_food_standard_distance']) / 5

df_all_data['factor_5'] = ((df_all_data['max_standard_distance'] - df_all_data['e_boredom_standard_distance']) +
  df_all_data['e_desire_sex_standard_distance']) / 2
