# -*- coding: utf-8 -*- 

import logging
import datetime

###############################################################################
# Topology      
###############################################################################
LOG_LEVEL = logging.DEBUG
LOG_FORMAT = '%(asctime)-15s %(filename)s %(levelname)-10s %(message)s'
ARRAYSIZE=100000
TOP_N = " "
AML_INPUT_DIR=r"c:\Tonbeller\SironAML\client\0001\data\input"
K1YC_INPUT_DIR=r"c:\Tonbeller\SironKYC\client\0001\data\input"
KYC_NET_DIR=r"s:\Exchange\customers"
SHARE=r'\\SironAml\Siron'
LAST_SCORING_DATE_FILE=r"last_scoring_date.txt"

DSN = 'VTBDB'
USER='aml'
PASSWORD = 'aml'

LOG_DSN = 'VTBDB'
LOG_USER='aml'
LOG_PASSWORD='aml'
       
OSUSER=r'test'
OSPASS='test'
###############################################################################
# Models
###############################################################################
IN_CUSTOMER_QUERY =\
"SELECT institute, custno, firstname, lastname, street, zip, town, h_country, s_country,\
   cusy, fk_csmno, profession, branch,\
   CASE /*hotfix*/\
          WHEN TO_CHAR(birthdate, 'YYYYMM') = '201809' THEN ADD_MONTHs(birthdate, -3)\
          WHEN birthdate > sysdate THEN TO_DATE('18610219', 'YYYYMMDD')\
          ELSE birthdate\
       END AS BIRTHDATE,\
       CASE /*hotfix*/\
          WHEN TO_CHAR(custcontact, 'YYYYMM') = '201809' THEN ADD_MONTHs(custcontact, -3)\
          WHEN custcontact > sysdate THEN TO_DATE('18610219', 'YYYYMMDD')\
          ELSE custcontact\
       END AS custcontact,\
   exemptionflag,\
   exemptionamount, asylsyn, salary, salarydate, nat_country, tot_wealth,\
   prop_wealth, branch_office, cust_type, cust_flag_01, cust_flag_02, cust_flag_03,\
   emplno, pass_no, birth_country, birth_place, borroweryn, direct_debityn, gender,\
   risk_class, cust_sp_01, cust_sp_02, cust_sp_03, cust_sp_04, cust_sp_05,\
   TO_CHAR(cust_sp_06, 'YYYYMMDD') AS cust_sp_06, \
   cust_sp_07, cust_sp_08, cust_sp_09, cust_sp_10, cust_np_1,\
   cust_np_2, cust_np_3, cust_np_4, cust_np_5, cust_sph_01, cust_sph_02,\
   cust_sph_03, cust_sph_04, cust_sph_05, cust_nph_1, cust_nph_2, cust_nph_3,\
   cust_nph_4, cust_nph_5 \
  FROM customer \
 ORDER BY institute, custno"
# data quality erroACCOPENINGrs - early ` and NULL full_name
#    WHERE custno in ('1023494', '1025416', '1027010', '1030341', '7801472')\ 

################################################################################
# field_name: (Seq, Len, Mandatory, Key, Alias, DataType)
IN_CUSTOMER_ROW =\
    {"institute": (1,  4,  True,  True,  'institute', 'char'),\
     "custno":    (2,  16, True,  True,  'custno',    'char'),\
     "firstname": (3,  32, False, False, 'firstname', 'char'),\
     "lastname":  (4,  32, True,  False, 'lastname',  'char'),\
     "street":    (5,  32, False, False, 'street',    'char'),\
     "zip":       (6,  7,  False, False, 'zip',       'char'),\
     "town":      (7,  28, False, False, 'town',      'char'),\
     "h_country": (8,  3,  False, False, 'h_country', 'char'),\
     "s_country": (9,  3,  False, False, 's_country', 'char'),\
     "cusy":      (10, 8,  False, False, '',          'char'),\
     "fk_csmno":  (11, 12, False, False, '',          'char'),\
     "profession":(12, 32, False, False, 'profession', 'char'),\
     "branch":    (13, 32, False, False, '',            'char'),\
     "birthdate": (14,  8, False, False, 'birthdate', 'date'),\
     "custcontact":    (15,  8, False, False, 'custcontact',   'date'),\
     "exemptionflag":  (16,  1, False, False, '',      'char'),\
     "exemptionamount":(17, 11, False, False, '',      'char'),\
     "asylsyn":    (18,  1, False, False, '',          'char'),\
     "salary":     (19, 17, False, False, '',          'number'),\
     "salarydate": (20,  8, False, False, '',          'date'),\
     "nat_country":(21,  3, False, False, '',          'char'),\
     "tot_wealth": (22, 17, False, False, '',          'number'),\
     "prop_wealth":(23,  3, False, False, '',          'number'),\
     "branch_office":(24, 10, False, False, 'branch_office', 'char'),\
     "cust_type":  (25,  1, False, False, 'cust_type',    'char'),\
     "cust_flag01":(26,  1, False, False, 'cust_flag_01', 'char'),\
     "cust_flag02":(27,  1, False, False, 'cust_flag_02', 'char'),\
     "cust_flag03":(28,  1, False, False, 'cust_flag_03', 'char'),\
     "cust_flag04":(29,  1, False, False, '',             'char'),\
     "cust_flag05":(30,  1, False, False, '',             'char'),\
     "cust_flag06":(31,  1, False, False, '',             'char'),\
     "cust_flag07":(32,  1, False, False, '',             'char'),\
     "cust_flag08":(33,  1, False, False, '',             'char'),\
     "cust_flag09":(34,  1, False, False, '',             'char'),\
     "cust_flag10":(35,  1, False, False, '',             'char'),\
     "cust_flag11":(36,  1, False, False, '',             'char'),\
     "cust_flag12":(37,  1, False, False, '',             'char'),\
     "cust_flag13":(38,  1, False, False, '',             'char'),\
     "cust_flag14":(39,  1, False, False, '',             'char'),\
     "cust_flag15":(40,  1, False, False, '',             'char'),\
     "cust_flag16":(41,  1, False, False, '',             'char'),\
     "cust_flag17":(42,  1, False, False, '',             'char'),\
     "cust_flag18":(43,  1, False, False, '',             'char'),\
     "cust_flag19":(44,  1, False, False, '',             'char'),\
     "cust_flag20":(45,  1, False, False, '',             'char'),\
     "cust_flag21":(46,  1, False, False, '',             'char'),\
     "cust_flag22":(47,  1, False, False, '',             'char'),\
     "cust_flag23":(48,  1, False, False, '',             'char'),\
     "cust_flag24":(49,  1, False, False, '',             'char'),\
     "emplno":     (50, 16, False, False, '',       'char'),\
     "pass_no":    (51, 17, False, False, 'pass_no',      'char'),\
     "birth_country":(52, 3, False, False, 'birth_country', 'char'),\
     "birth_place":(53, 32, False, False, 'birth_place',   'char'),\
     "borroweryn": (54,  1, False, False, 'borroweryn',    'char'),\
     "direct_debityn": (55, 1, False, False, 'direct_debityn', 'char'),\
     "gender":     (56,  1, False, False, 'gender',        'char'),\
     "risk_class": (57, 10, False, False, 'risk_class',    'char')\
    }                   

IN_CUSTOMER_EXTENSION_NOHIST_QUERY =\
   "SELECT INSTITUTE, CUSTNO, CUST_SP_01, CUST_SP_02, CUST_SP_03, CUST_SP_04, \
           CUST_SP_05,\
        CASE /*hotfix*/\
          WHEN TO_CHAR(birthdate, 'YYYYMM') = '201809' THEN ADD_MONTHs(birthdate, -3)\
          WHEN birthdate > sysdate THEN TO_DATE('18610219', 'YYYYMMDD')\
          ELSE birthdate\
       END AS cust_sp_06,\
       CUST_SP_07, CUST_SP_08, CUST_SP_09, CUST_SP_10, \
       CUST_NP_1, CUST_NP_2, CUST_NP_3, CUST_NP_4, CUST_NP_5 \
      FROM customer \
       WHERE COALESCE(CUST_SP_01, CUST_SP_02, CUST_SP_03, CUST_SP_04,CUST_SP_05, CUST_SP_07, CUST_SP_08, CUST_SP_09, CUST_SP_10) IS NOT NULL \
	   ORDER BY INSTITUTE, CUSTNO"
     
# field_name: (Seq, Len, Mandatory, Key, Alias, DataType)     
IN_CUSTOMER_EXTENSION_NOHIST_ROW =\
    {"institute":  ( 1,  4,  True,  True,  'institute',  'char'),\
     "custno":     ( 2,  16, True,  True,  'custno',     'char'),\
     "cust_sp_01": ( 3,  64, False, False, 'cust_sp_01', 'char'),\
     "cust_sp_02": ( 4,  64, False, False, 'cust_sp_02', 'char'),\
     "cust_sp_03": ( 5,  64, False, False, 'cust_sp_03', 'char'),\
     "cust_sp_04": ( 6,  64, False, False, 'cust_sp_04', 'char'),\
     "cust_sp_05": ( 7,  64, False, False, 'cust_sp_05', 'char'),\
     "cust_sp_06": ( 8,  64, False, False, 'cust_sp_06', 'date'),\
     "cust_sp_07": ( 9,  64, False, False, 'cust_sp_07', 'char'),\
     "cust_sp_08": (10,  64, False, False, 'cust_sp_08', 'char'),\
     "cust_sp_09": (11,  64, False, False, 'cust_sp_09', 'char'),\
     "cust_sp_10": (12,  64, False, False, '', 'char'),\
     "cust_sp_11": (13,  64, False, False, '', 'char'),\
     "cust_sp_12": (14,  64, False, False, '', 'char'),\
     "cust_sp_13": (15,  64, False, False, '', 'char'),\
     "cust_sp_14": (16,  64, False, False, '', 'char'),\
     "cust_sp_15": (17,  64, False, False, '', 'char'),\
     "cust_sp_16": (18,  64, False, False, '', 'char'),\
     "cust_sp_17": (19,  64, False, False, '', 'char'),\
     "cust_sp_18": (20,  64, False, False, '', 'char'),\
     "cust_sp_19": (21,  64, False, False, '', 'char'),\
     "cust_sp_20": (22,  64, False, False, '', 'char'),\
     "cust_sp_21": (23,  64, False, False, '', 'char'),\
     "cust_sp_22": (24,  64, False, False, '', 'char'),\
     "cust_sp_23": (25,  64, False, False, '', 'char'),\
     "cust_sp_24": (26,  64, False, False, '', 'char'),\
     "cust_sp_25": (27,  64, False, False, '', 'char'),\
     "cust_sp_26": (28,  64, False, False, '', 'char'),\
     "cust_sp_27": (29,  64, False, False, '', 'char'),\
     "cust_sp_28": (30,  64, False, False, '', 'char'),\
     "cust_sp_29": (31,  64, False, False, '', 'char'),\
     "cust_sp_30": (32,  64, False, False, '', 'char'),\
     "cust_sp_31": (33,  64, False, False, '', 'char'),\
     "cust_sp_32": (34,  64, False, False, '', 'char'),\
     "cust_sp_33": (35,  64, False, False, '', 'char'),\
     "cust_sp_34": (36,  64, False, False, '', 'char'),\
     "cust_sp_35": (37,  64, False, False, '', 'char'),\
     "cust_sp_36": (38,  64, False, False, '', 'char'),\
     "cust_sp_37": (39,  64, False, False, '', 'char'),\
     "cust_sp_38": (40,  64, False, False, '', 'char'),\
     "cust_sp_39": (41,  64, False, False, '', 'char'),\
     "cust_sp_40": (42,  64, False, False, '', 'char'),\
     "cust_sp_41": (43,  64, False, False, '', 'char'),\
     "cust_sp_42": (44,  64, False, False, '', 'char'),\
     "cust_sp_43": (45,  64, False, False, '', 'char'),\
     "cust_sp_44": (45,  64, False, False, '', 'char'),\
     "cust_sp_45": (45,  64, False, False, '', 'char')\
    }

IN_CUSTOMER_EXTENSION_HIST_QUERY =\
   "SELECT " + TOP_N + "\
     institute,\
     custno,\
     cust_sph_01 AS cust_sph_01,\
     cust_sph_02 AS cust_sph_02,\
     cust_sph_03 AS cust_sph_03\
   FROM am.customer_extension_1\
   ORDER BY institute, custno"

# field_name: (Seq, Len, Mandatory, Key, Alias, DataType)   
IN_CUSTOMER_EXTENSION_HIST_ROW =\
    {"institute":   ( 1,  4,  True,  True,  'institute',   'char'),\
     "custno":      ( 2,  16, True,  True,  'custno',      'char'),\
     "cust_sph_01": ( 3,  64, False, False, 'cust_sph_01', 'char'),\
     "cust_sph_02": ( 4,  64, False, False, 'cust_sph_02', 'char'),\
     "cust_sph_03": ( 5,  64, False, False, '',            'char'),\
     "cust_sph_04": ( 6,  64, False, False, '',            'char'),\
     "cust_sph_05": ( 7,  64, False, False, '',            'char'),\
     "cust_sph_06": ( 8,  64, False, False, '',            'char'),\
     "cust_sph_07": ( 9,  64, False, False, '',            'char'),\
     "cust_sph_08": (10,  64, False, False, '',            'char'),\
     "cust_sph_09": (11,  64, False, False, '',            'char'),\
     "cust_sph_10": (12,  64, False, False, '',            'char'),\
     "cust_sph_11": (13,  64, False, False, '',            'char'),\
     "cust_sph_12": (14,  64, False, False, '',            'char'),\
     "cust_sph_13": (15,  64, False, False, '',            'char'),\
     "cust_sph_14": (16,  64, False, False, '',            'char'),\
     "cust_sph_15": (17,  64, False, False, '',            'char'),\
     "cust_sph_16": (18,  64, False, False, '',            'char'),\
     "cust_sph_17": (19,  64, False, False, '',            'char'),\
     "cust_sph_18": (20,  64, False, False, '',            'number'),\
     "cust_sph_19": (21,  64, False, False, '',            'number'),\
     "cust_sph_20": (22,  64, False, False, '',            'number'),\
     "cust_sph_21": (23,  64, False, False, '',            'number'),\
     "cust_sph_22": (24,  64, False, False, '',            'number'),\
     "cust_sph_23": (25,  64, False, False, '',            'number'),\
     "cust_sph_24": (26,  64, False, False, '',            'number'),\
     "cust_sph_25": (27,  64, False, False, '',            'number'),\
     "cust_sph_26": (28,  64, False, False, '',            'number'),\
     "cust_sph_27": (29,  64, False, False, '',            'number'),\
     "cust_sph_28": (30,  64, False, False, '',            'number'),\
     "cust_sph_29": (31,  64, False, False, '',            'number'),\
     "cust_sph_30": (32,  64, False, False, '',            'number'),\
     "cust_sph_31": (33,  64, False, False, '',            'number'),\
     "cust_sph_32": (34,  64, False, False, '',            'number'),\
     "cust_sph_33": (35,  64, False, False, '',            'number'),\
     "cust_sph_34": (36,  64, False, False, '',            'number'),\
     "cust_sph_35": (37,  64, False, False, '',            'number'),\
     "cust_sph_36": (38,  64, False, False, '',            'number'),\
     "cust_sph_37": (39,  64, False, False, '',            'number'),\
     "cust_sph_38": (40,  64, False, False, '',            'number'),\
     "cust_sph_39": (41,  64, False, False, '',            'number'),\
     "cust_sph_40": (42,  64, False, False, '',            'number'),\
     "cust_sph_41": (43,  64, False, False, '',            'number'),\
     "cust_sph_42": (44,  64, False, False, '',            'number'),\
     "cust_sph_43": (45,  64, False, False, '',            'number'),\
     "cust_sph_44": (46,  64, False, False, '',            'number'),\
     "cust_sph_45": (47,  64, False, False, '',            'number'),\
     "cust_sph_46": (48,  64, False, False, '',            'number'),\
     "cust_sph_47": (49,  64, False, False, '',            'number'),\
     "cust_sph_48": (50,  64, False, False, '',            'number'),\
     "cust_nph_01": (51,  38, False, False, '',            'number')
        }

IN_COUNTRY_QUERY =\
   "SELECT  1 AS CUSTNO, to_char(countryint) as COUNTRYINT, countryext, name\
       FROM country\
     ORDER BY countryext"
    
IN_COUNTRY_ROW =\
    {"countryint":   (1,  3,  True,  True, 'countryint',    'char'),\
     "countryext":   (2,  3, True,  True,  'countryext',    'char'),\
     "desc":         (3,128, True,  True,  'name',          'char')\
    }
# Во время работы выгрузчика WHERE 1=1 заменяется на 1-й и последний день месяца:
# WHERE transtimestamp BETWEEN DATE('2015-05-01') AND DATETIME('2015-05-31 23:59:59')\    
IN_TRANSACTION_QUERY =\
   "SELECT " + TOP_N + "\
      CUST_INSTITUTE,\
      CUST_CUSTNO AS CUSTNO,\
      ACC_BUSINESSTYPE,\
      ACC_ACCNO,\
      ACC_BUSINESSNO,\
      ACC_CURRENCYISO,\
      ENTRYDATE,\
      VALUEDATE,\
      BUSINESSNO_TRANS,\
	  TXTKEY AS TXTKEY,\
	  PRN,\
      FK_CURRENCY,\
      AMOUNT,\
      AMOUNTORIG,\
      BRANCH_OFFICE,\
      CONTRA_COUNTRY,\
      CONTRA_ACCNO,\
	  CONTRA_ZIP,\
      CONTRA_NAME,\
      CSHYN,\
      REASON1,\
      REASON2,\
      REASON3,\
      REASON4,\
      TRANSTIMESTAMP,\
      CONTRA_CUSTNO,\
	  ANALYTICAL_TRANS_CODE,\
	  EARLY_LIQUIDATION_FLAG,\
	  CONTRA_EMPLNO,\
	  CONTRA_ACC_OWNER_FLAG,\
	  OWNER_EMPLNO,\
      CONTRA_BUSINESSTYPE,\
      CONTRA_H_ACCNO,\
      CONTRA_BUSINESSNO,\
      CONTRA_ACC_CURRENCYISO,\
      FILLER\
     FROM TRANSACTION t\
    WHERE 1=1\
    ORDER BY 1,2,3,4,5,6,7,8,9"
    

    
# field_name: (Seq, Len, Mandatory, Key, Alias, DataType)
IN_TRANSACTION_ROW =\
    {"cust_institute":   (1,  4,  True,  True,  'cust_institute',    'char'),\
     "cust_custno":      (2,  16, True,  True,  'custno',            'char'),\
     "acc_businesstype": (3,   4, False, False, 'acc_businesstype',  'char'),\
     "acc_accno":        (4,  11, True,  False, 'acc_accno',         'char'),\
     "acc_businessno":   (5,  11, False, False, 'acc_businessno',    'char'),\
     "acc_currencyiso":  (6,   3, False, False, 'acc_currencyiso',   'char'),\
     "entrydate":        (7,   8, False, False, 'entrydate',         'date'),\
     "valuedate":        (8,   8, False, False, 'valuedate',         'date'),\
     "businessno_trans": (9,  16, False, False, 'businessno_trans',  'char'),\
     "txtkey":           (10,  2, False, False, 'txtkey',            'char'),\
     "prn":              (11,  8, False, False, 'prn',               'char'),\
     "fk_currency":      (12,  3, False, False, 'fk_currency',       'char'),\
     "amount":           (13, 17, False, False, 'amount',            'number'),\
     "amountorig":       (14, 17, False, False, 'amountorig',        'number'),\
     "branch_office":    (15, 10, False, False, 'branch_office',     'char'),\
     "contra_country":   (16,  3, False, False, 'contra_country',    'char'),\
     "contra_accno":     (17, 35, False, False, 'contra_accno',      'char'),\
     "contra_zip":       (18, 12, False, False, 'contra_zip',        'char'),\
     "contra_name":      (19, 27, False, False, 'contra_name',       'char'),\
     "cshyn":            (20,  1, False, False, 'cshyn',             'char'),\
     "reason1":          (21, 27, False, False, 'reason1',           'char'),\
     "reason2":          (22, 27, False, False, 'reason2',           'char'),\
     "reason3":          (23, 27, False, False, 'reason3',           'char'),\
     "reason4":          (24, 27, False, False, 'reason4',           'char'),\
     "status":           (25,  1, False, False, '',                  'char'),\
     "tr_flag_01":       (26,  1, False, False, '',                  'char'),\
     "tr_flag_02":       (27,  1, False, False, '',                  'char'),\
     "tr_flag_03":       (28,  1, False, False, '',                  'char'),\
     "tr_flag_04":       (29,  1, False, False, '',                  'char'),\
     "tr_flag_05":       (30,  1, False, False, '',                  'char'),\
     "tr_flag_06":       (31,  1, False, False, '',                  'char'),\
     "tr_flag_07":       (32,  1, False, False, '',                  'char'),\
     "tr_flag_08":       (33,  1, False, False, '',                  'char'),\
     "tr_flag_09":       (34,  1, False, False, '',                  'char'),\
     "tr_flag_10":       (35,  1, False, False, '',                  'char'),\
     "tr_flag_11":       (36,  1, False, False, '',                  'char'),\
     "tr_flag_12":       (37,  1, False, False, '',                  'char'),\
     "tr_flag_13":       (38,  1, False, False, '',                  'char'),\
     "tr_flag_14":       (39,  1, False, False, '',                  'char'),\
     "tr_flag_15":       (40,  1, False, False, '',                  'char'),\
     "tr_flag_16":       (41,  1, False, False, '',                  'char'),\
     "tr_flag_17":       (42,  1, False, False, '',                  'char'),\
     "tr_flag_18":       (43,  1, False, False, '',                  'char'),\
     "tr_flag_19":       (44,  1, False, False, '',                  'char'),\
     "tr_flag_20":       (45,  1, False, False, '',                  'char'),\
     "tr_flag_21":       (46,  1, False, False, '',                  'char'),\
     "tr_flag_22":       (47,  1, False, False, '',                  'char'),\
     "tr_flag_23":       (48,  1, False, False, '',                  'char'),\
     "tr_flag_24":       (49,  1, False, False, '',                  'char'),\
     "tr_emplno":        (50, 16, False, False, '',         'char'),
     "transtimestamp":   (51, 17, False, False, 'transtimestamp',    'char'),\
     "contra_custno":    (52, 16, False, False, 'contra_custno',     'char'),\
     "analytical_trans_code":(53,  6, False, False, 'analytical_trans_code',         'char'),\
     "early_liquidation_flag":(54,  1, False, False, 'cshyn',  'char'),\
     "contra_emplno":   (55,  16, False, False, '', 'char'),\
     "contra_acc_owner_flag":   (56,  1, False, False, 'contra_acc_owner_flag', 'char'),\
     "owner_emplno":   (57,  16, False, False, '',       'char'),\
     "contra_businesstype":(58,4, False, False,'contra_businesstype','char'),\
     "contra_h_accno":    (59, 11, False, False, 'contra_h_accno',   'char'),\
     "contra_businessno": (60, 11, False, False, 'contra_businessno','char'),\
     "contra_acc_currencyiso":(61,3,False,False,'contra_acc_currencyiso','char'),\
     "filler":           (62,  7, False, False, '',                 'char')
    }

IN_TRANSACTION_EXTENSION_QUERY =\
   "SELECT " + TOP_N + "\
      CUST_INSTITUTE,\
      CUST_CUSTNO AS CUSTNO,\
      ACC_BUSINESSTYPE,\
      ACC_ACCNO,\
      ACC_BUSINESSNO,\
      ACC_CURRENCYISO,\
      ENTRYDATE,\
	  VALUEDATE,\
      BUSINESSNO_TRANS,\
      TR_SP_01,\
      TR_SP_02,\
      TR_SP_03,\
      TR_SP_04,\
      TR_SP_05,\
      TR_SP_06,\
      TR_SP_07,\
      TR_SP_08,\
      TR_SP_09,\
      TR_SP_10\
     FROM TRANSACTION t\
    WHERE 1=1 \
      AND (TR_SP_01 IS NOT NULL OR TR_SP_02 IS NOT NULL OR TR_SP_03 IS NOT NULL OR TR_SP_04 IS NOT NULL OR TR_SP_05 IS NOT NULL OR TR_SP_06 IS NOT NULL OR TR_SP_07 IS NOT NULL OR TR_SP_08 IS NOT NULL OR TR_SP_09 IS NOT NULL OR TR_SP_10 IS NOT NULL) \
    ORDER BY 1,2,3,4,5,6,7,8,9"

IN_TRANSACTION_EXTENSION_ROW =\
    {"cust_institute":   (1,  4,  True,  True,  'cust_institute',    'char'),\
     "cust_custno":      (2,  16, True,  True,  'custno',            'char'),\
     "acc_businesstype": (3,   4, False, False, 'acc_businesstype',  'char'),\
     "acc_accno":        (4,  11, True,  False, 'acc_accno',         'char'),\
     "acc_businessno":   (5,  11, False, False, 'acc_businessno',    'char'),\
     "acc_currencyiso":  (6,   3, False, False, 'acc_currencyiso',   'char'),\
     "entrydate":        (7,   8, False, False, 'entrydate',         'date'),\
     "valuedate":        (8,   8, False, False, 'valuedate',         'date'),\
     "businessno_trans": (9,  16, False, False, 'businessno_trans',  'char'),\
     "tr_sp_01":         (10, 64, False, False, 'tr_sp_01',          'char'),\
     "tr_sp_02":         (11, 64, False, False, 'tr_sp_02',          'char'),\
     "tr_sp_03":         (12, 64, False, False, 'tr_sp_03',          'char'),\
     "tr_sp_04":         (13, 64, False, False, 'tr_sp_04',          'char'),\
     "tr_sp_05":         (14, 64, False, False, 'tr_sp_05',          'char'),\
     "tr_sp_06":         (15, 64, False, False, 'tr_sp_06',          'char'),\
     "tr_sp_07":         (16, 64, False, False, 'tr_sp_07',          'char'),\
     "tr_sp_08":         (17, 64, False, False, 'tr_sp_08',          'char'),\
     "tr_sp_09":         (18, 64, False, False, 'tr_sp_09',          'char'),\
     "tr_sp_10":         (19, 64, False, False, 'tr_sp_10',          'char'),\
    }

IN_ACCOUNT_QUERY =\
   "SELECT " + TOP_N + "\
     CUST_INSTITUTE,\
     CUST_CUSTNO as custno,\
     BUSINESSTYPE,\
     ACCNO,\
     BUSINESSNO,\
     ACC_CURRENCYISO,\
     /*hotfix*/ CASE WHEN ACCOPENING > SYSDATE THEN TO_DATE('19.02.1861', 'DD.MM.YYYY') ELSE NVL(ACCOPENING, TO_DATE('19.02.1861', 'DD.MM.YYYY')) END AS ACCOPENING,\
     ACCCLOSE,\
	 ACCLIMIT,\
	 ACCBALANCE,\
	 SUMCREDRUNYEAR,\
	 SUMDEBRUNYEAR,\
	 NUMBERACCOUNTS,\
	 ACC_FLAG_01,\
	 ACC_FLAG_02,\
	 ACC_FLAG_03,\
	 PODTYPE,\
	 IBAN,\
	 ACC_TYPE,\
	 EMPLNO,\
	 PURPOSE\
    FROM ACCOUNT\
   ORDER BY 1,2,3,4,5,6"
   
# field_name: (Seq, Len, Mandatory, Key, Alias, DataType)     
IN_ACCOUNT_ROW =\
    {"cust_institute":    ( 1,  4,  True,  True,  'cust_institute',   'char'),\
     "cust_custno":       ( 2,  16, True,  True,  'custno',           'char'),\
     "businesstype":      ( 3,   4, False, False, 'businesstype',     'char'),\
     "accno":             ( 4,  11, False, False, 'accno',            'char'),\
     "businessno":        ( 5,  11, False, False, 'businessno',       'char'),\
     "acc_currencyiso":   ( 6,   3, False, False, 'acc_currencyiso',  'char'),\
     "accopening":        ( 7,   8, False, False, 'accopening',       'date'),\
     "accclose":          ( 8,   8, False, False, 'accclose',         'date'),\
     "acchold_institute": ( 9,   4, False, False, '',                 'char'),\
     "acchold_custno":    (10,  16, False, False, '',                 'char'),\
     "acclimit":          (11,  17, False, False, '',                 'char'),\
     "accbalance":        (12,  17, False, False, 'accbalance',       'number'),\
     "sumcredrunyear":    (13,  17, False, False, 'sumcredrunyear',   'number'),\
     "sumdebrunyear":     (14,  17, False, False, 'sumdebrunyear',    'number'),\
     "numberaccounts":    (15,   4, False, False, '',                 'number'),\
     "acc_flag_01":       (16,   1, False, False, 'acc_flag_01',      'char'),\
     "acc_flag_02":       (17,   1, False, False, '',                 'char'),\
     "acc_flag_03":       (18,   1, False, False, '',                 'char'),\
     "acc_flag_04":       (19,   1, False, False, '',                 'char'),\
     "acc_flag_05":       (20,   1, False, False, '',                 'char'),\
     "acc_flag_06":       (21,   1, False, False, '',                 'char'),\
     "acc_flag_07":       (22,   1, False, False, '',                 'char'),\
     "acc_flag_08":       (23,   1, False, False, '',                 'char'),\
     "acc_flag_09":       (24,   1, False, False, '',                 'char'),\
     "acc_flag_10":       (25,   1, False, False, '',                 'char'),\
     "acc_flag_11":       (26,   1, False, False, '',                 'char'),\
     "acc_flag_12":       (27,   1, False, False, '',                 'char'),\
     "acc_flag_13":       (28,   1, False, False, '',                 'char'),\
     "acc_flag_14":       (29,   1, False, False, '',                 'char'),\
     "acc_flag_15":       (30,   1, False, False, '',                 'char'),\
     "acc_flag_16":       (31,   1, False, False, '',                 'char'),\
     "acc_flag_17":       (32,   1, False, False, '',                 'char'),\
     "acc_flag_18":       (33,   1, False, False, '',                 'char'),\
     "acc_flag_19":       (34,   1, False, False, '',                 'char'),\
     "acc_flag_20":       (35,   1, False, False, '',                 'char'),\
     "acc_flag_21":       (36,   1, False, False, '',                 'char'),\
     "acc_flag_22":       (37,   1, False, False, '',                 'char'),\
     "acc_flag_23":       (38,   1, False, False, '',                 'char'),\
     "acc_flag_24":       (39,   1, False, False, '',                 'char'),\
     "podtype":           (40,   5, False, False, 'podtype',          'char'),\
     "iban":              (41,  35, False, False, 'iban',             'char'),\
     "acc_type":          (42,   1, False, False, 'acc_type',         'char'),\
     "hold_mail":         (43,   1, False, False, '',                 'char'),\
     "emplno":            (44,  16, False, False, 'emplno',           'char'),\
     "purpose":           (45,  32, False, False, 'purpose',          'char')\
    }

IN_ACCOUNT_EXTENSION_NOHIST_QUERY =\
   "SELECT " + TOP_N + "\
     CUST_INSTITUTE,\
     CUST_CUSTNO AS CUSTNO,\
     BUSINESSTYPE,\
     ACCNO,\
     BUSINESSNO,\
     ACC_CURRENCYISO,\
	 ACC_SP_01,\
	 ACC_SP_02,\
	 ACC_SP_03,\
	 ACC_SP_04,\
	 ACC_SP_05,\
	 ACC_SP_06,\
	 ACC_SP_07,\
	 ACC_SP_08,\
	 ACC_SP_09,\
	 ACC_SP_10,\
	 ACC_NP_01,\
	 ACC_NP_02,\
	 ACC_NP_03,\
	 ACC_NP_04,\
	 ACC_NP_05,\
	 ACC_SPH_01,\
	 ACC_SPH_02,\
	 ACC_SPH_03,\
	 ACC_SPH_04,\
	 ACC_SPH_05,\
	 ACC_NPH_01,\
	 ACC_NPH_02,\
	 ACC_NPH_03,\
	 ACC_NPH_04,\
	 ACC_NPH_05\
   FROM ACCOUNT\
   ORDER BY 1,2,3,4,5,6"

# field_name: (Seq, Len, Mandatory, Key, Alias, DataType)     
IN_ACCOUNT_EXTENSION_NOHIST_ROW =\
    {"institute":      ( 1,  4,  True,  True,  'cust_institute', 'char'),\
     "cust_custno":    ( 2,  16, True,  True,  'custno',         'char'),\
     "businesstype":   ( 3,   4, False, False, 'businesstype',   'char'),\
     "accno":          ( 4,  11, False, False, 'accno',          'char'),\
     "businessno":     ( 5,  11, False, False, 'businessno',     'char'),\
     "acc_currencyiso":( 6,   3, False, False, 'acc_currencyiso','char'),\
     "acc_sp_01":      ( 7,  64, False, False, 'acc_sp_01',      'char'),\
     "acc_sp_02":      ( 8,  64, False, False, 'acc_sp_02',      'char')\
    }

  

IN_BENEFICIAL_OWNER_QUERY=\
   "SELECT \
     '\"'||INSTITUTE||'\"'  AS INSTITUTE,\
     '\"'||CUSTNO||'\"' AS CUSTNO,\
     '\"'||REL_CUSTNO||'\"' AS REL_CUSTNO,\
     '\"'||REL_TYPE||'\"' AS REL_TYPE,\
     NVL(CAST(REL_SHARE AS VARCHAR2(10)), '0') || '.0' AS REL_SHARE,\
     '\"'||REL_FLAG1||'\"' AS REL_FLAG1,\
     '\"'||REL_FLAG2||'\"' AS REL_FLAG2,\
     '\"'||REL_FLAG3||'\"' AS REL_FLAG3,\
     '\"'||REL_COMMENT||'\"' AS REL_COMMENT,\
     '\"'||PROCESSFLAG||'\"' AS PROCESSFLAG\
   FROM BENEFICIAL_OWNERS\
   ORDER BY 1,2,3,4"

# field_name: (Seq, Len, Mandatory, Key, Alias, DataType)     
IN_BENEFICIAL_OWNER_ROW =\
    {"institute":      ( 1,   6, True,  True,  'INSTITUTE',  'char'),\
     "cust_custno":    ( 2,  18, True,  True,  'CUSTNO',     'char'),\
     "rel_custno":     ( 3,  18, False, False, 'REL_CUSTNO', 'char'),\
     "rel_type":       ( 4,   4, False, False, 'REL_TYPE',   'char'),\
     "rel_share":      ( 5,   9, False, False, 'REL_SHARE',  'char'),\
     "rel_flag1":      ( 6,   3, False, False, 'REL_FLAG1',  'char'),\
     "rel_flag2":      ( 7,   3, False, False, 'REL_FLAG2',  'char'),\
     "rel_flag3":      ( 8,   3, False, False, 'rel_flag3',  'char'),\
     "rel_comment":    ( 9,1154, False, False, 'REL_COMMENT','char'),\
     "processflag":    (10,   3, False, False, 'PROCESSFLAG','char')
    }

IN_POWER_OF_DISPOSAL_QUERY =\
      "SELECT " + TOP_N + "\
     CUST_INSTITUTE,\
     CUST_CUSTNO AS CUSTNO,\
     BUSINESSTYPE,\
     ACCNO,\
     BUSINESSNO,\
     ACC_CURRENCYISO,\
     POD_CUSTNO,\
     PODTYPE,\
	 ROLE_01,\
	 ROLE_02,\
	 ROLE_03\
    FROM POWER_OF_DISPOSAL\
   ORDER BY 1,2,3,4,5,6,7"

IN_POWER_OF_DISPOSAL_ROW =\
    {"cust_institute": ( 1,  4,  True,  True,  'CUST_INSTITUTE', 'char'),\
     "cust_custno":    ( 2,  16, True,  True,  'custno',         'char'),\
     "businesstype":   ( 3,   4, False, False, 'businesstype',   'char'),\
     "accno":          ( 4,  11, False, False, 'accno',          'char'),\
     "businessno":     ( 5,  11, False, False, 'businessno',     'char'),\
     "acc_currencyiso":( 6,   3, False, False, 'acc_currencyiso','char'),\
     "pod_custno":     ( 7,  16, False, False, 'pod_custno',     'char'),\
     "podtype":        ( 8,   5, False, False, 'podtype',        'char'),\
     "role_01":        ( 9,   1, False, False, '',               'char'),\
     "role_02":        (10,   1, False, False, '',               'char'),\
     "role_03":        (11,   1, False, False, '',               'char'),\
     "role_04":        (12,   1, False, False, '',               'char'),\
     "role_05":        (13,   1, False, False, '',               'char'),\
     "role_06":        (14,   1, False, False, '',               'char'),\
     "role_07":        (15,   1, False, False, '',               'char'),\
     "role_08":        (16,   1, False, False, '',               'char'),\
     "role_09":        (17,   1, False, False, '',               'char'),\
     "role_10":        (18,   1, False, False, '',               'char'),\
     "role_11":        (19,   1, False, False, '',               'char'),\
     "role_12":        (20,   1, False, False, '',               'char'),\
     "role_13":        (21,   1, False, False, '',               'char'),\
     "role_14":        (22,   1, False, False, '',               'char'),\
     "role_15":        (23,   1, False, False, '',               'char'),\
     "role_16":        (24,   1, False, False, '',               'char'),\
     "role_17":        (25,   1, False, False, '',               'char'),\
     "role_18":        (26,   1, False, False, '',               'char'),\
     "role_19":        (27,   1, False, False, '',               'char'),\
     "role_20":        (28,   1, False, False, '',               'char'),\
     "role_21":        (29,   1, False, False, '',               'char'),\
     "role_22":        (30,   1, False, False, '',               'char'),\
     "role_23":        (31,   1, False, False, '',               'char'),\
     "role_24":        (32,   1, False, False, '',               'char'),\
     "check_yn":       (33,   1, False, False, '',               'char')\
    }


IN_CUST_SERV_MANAGER_QUERY =\
    "SELECT INSTITUTE, 1 AS CUSTNO, CSMNO, CSMNAME, CSMMAIL\
       FROM MANAGER\
    ORDER BY INSTITUTE, CSMNO"
   
IN_CUST_SERV_MANAGER_ROW =\
    {"institute": ( 1,  4,  True,  True, 'INSTITUTE',  'char'),\
     "csmno":     ( 2, 12, True,  True,  'csmno',      'char'),\
     "csmname":   ( 3, 32, False, False, 'CSMNO',      'char'),\
     "csmmail":   ( 3, 64, False, False, 'CSMMAIL',    'char'),\
    } 

IN_CUSY_QUERY =\
   "SELECT " + TOP_N + "\
     0 as custno,\
     cusy,\
     text\
    FROM am.cusy\
   ORDER BY cusy"
   
IN_CUSY_ROW =\
    {"cusy":  ( 1,   8,  True,  True, 'cusy',  'char'),\
     "desc":  ( 2, 254, True,  True,  'text',  'char')
    } 	

now_date = datetime.datetime.now()
now = now_date.strftime('%Y%m%d_%H%M')