# -*- coding: utf-8 -*- 

import logging
import datetime

###############################################################################
# Topology      
###############################################################################
LOG_LEVEL = logging.DEBUG
LOG_FORMAT = '%(asctime)-15s %(filename)s %(levelname)-10s %(message)s'
ARRAYSIZE=1000
TOP_N = " "
AML_INPUT_DIR=r"c:\Tonbeller\SironAML\client\0001\data\input"
KYC_INPUT_DIR=r"c:\Tonbeller\SironKYC\client\0001\data\input"
KYC_NET_DIR=r"s:\Exchange\customers"
SHARE=r'\\SironEmbargo\Siron'
LAST_SCORING_DATE_FILE=r"last_scoring_date.txt"

DRIVER = 'SQL Server'
#SERVER = '192.168.0.122' #Test
SERVER = '192.168.0.154'
#DATABASE='ABAHFTEST-RPT'
DATABASE='ABAPROD-RPT'
USER='SironAML'
#PASSWORD='Password1' #Test
PASSWORD = '$ironAML'

LOG_SERVER = '192.168.0.205'
LOG_DATABASE='LOGDATEN'
LOG_USER='AML01'
LOG_PASSWORD='siron'
       
OSUSER=r'ammb\mihail_kozyr'
OSPASS='amlp@ssw0rd2015'
###############################################################################
# Models
###############################################################################
IN_CUSTOMER_QUERY =\
   "SELECT " + TOP_N + "\
      INSTITUTE,\
      CUSTNO CUSTNO,\
      FIRSTNAME AS FIRSTNAME,\
	  LASTNAME  AS LASTNAME,\
      SUBSTRING(STREET, 1, 32) AS STREET,\
      ZIP AS ZIP,\
	  TOWN AS TOWN,\
	  H_COUNTRY AS H_COUNTRY,\
	  S_COUNTRY AS S_COUNTRY,\
	  CUSY AS CUSY,\
	  FK_CSMNO AS FK_CSMNO,\
	  SUBSTRING(PROFESSION, 1, 32) AS PROFESSION,\
	  BRANCH AS BRANCH,\
	  CAST(BIRTHDATE AS char(8)) AS BIRTHDATE,\
	  CAST(CUSTCONTACT AS char(8)) AS CUSTCONTACT,\
	  EXEMPTIONFLAG AS EXEMPTIONFLAG,\
	  EXEMPTIONAMOUNT AS EXEMPTIONAMOUNT,\
	  ASYLSYN AS ASYLSYN,\
	  SALARY AS SALARY,\
	  SALARYDATE AS SALARYDATE,\
	  NAT_COUNTRY AS NAT_COUNTRY,\
	  TOT_WEALTH AS TOT_WEALTH,\
	  PROP_WEALTH AS PROP_WEALTH,\
	  SUBSTRING(BRANCH_OFFICE, 1, 10) AS BRANCH_OFFICE,\
	  CUST_TYPE AS CUST_TYPE,\
	  CUST_FLAG_24 AS CUST_FLAG_24,\
	  EMPLNO AS EMPLNO,\
	  PASS_NO  AS PASS_NO,\
	  BIRTH_COUNTRY AS BIRTH_COUNTRY,\
	  SUBSTRING(BIRTH_PLACE, 1, 32) AS BIRTH_PLACE,\
	  BORROWERYN AS BORROWERYN,\
	  DIRECT_DEBITYN AS DIRECT_DEBITYN,\
	  GENDER AS GENDER,\
	  RISK_CLASS AS RISK_CLASS\
     FROM dbo.in_Customer\
     WHERE 1=1\
    ORDER BY institute, custno"

# data queality errors - early ` and NULL full_name
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
     "cusy":      (10, 8,  False, False, 'cusy',      'char'),\
     "fk_csmno":  (11, 12, False, False, 'fk_csmno',  'char'),\
     "profession":(12, 32, False, False, 'profession','char'),\
     "branch":    (13, 32, False, False, 'branch',    'char'),\
     "birthdate": (14,  8, False, False, 'birthdate', 'date'),\
     "custcontact":    (15,  8, False, False, 'custcontact',   'date'),\
     "exemptionflag":  (16,  1, False, False, 'exemptionflag', 'char'),\
     "exemptionamount":(17, 11, False, False, '','char'),\
     "asylsyn":    (18,  1, False, False, 'asylsyn',      'char'),\
     "salary":     (19, 17, False, False, 'salary',       'number'),\
     "salarydate": (20,  8, False, False, 'salarydate',   'date'),\
     "nat_country":(21,  3, False, False, 'nat_country',  'char'),\
     "tot_wealth": (22, 17, False, False, 'tot_wealth',   'number'),\
     "prop_wealth":(23,  3, False, False, '',  'number'),\
     "branch_office":(24, 10, False, False, 'branch_office', 'char'),\
     "cust_type":  (25,  1, False, False, 'cust_type',    'char'),\
     "cust_flag01":(26,  1, False, False, '',             'char'),\
     "cust_flag02":(27,  1, False, False, '',             'char'),\
     "cust_flag03":(28,  1, False, False, '',             'char'),\
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
     "cust_flag24":(49,  1, False, False, 'cust_flag_24', 'char'),\
     "emplno":     (50, 16, False, False, 'emplno',       'char'),\
     "pass_no":    (51, 17, False, False, 'pass_no',      'char'),\
     "birth_country":(52, 3, False, False, 'birth_country', 'char'),\
     "birth_place":(53, 32, False, False, 'birth_place',   'char'),\
     "borroweryn": (54,  1, False, False, 'borroweryn',    'char'),\
     "direct_debityn": (55, 1, False, False, 'direct_debityn', 'char'),\
     "gender":     (56,  1, False, False, 'gender',        'char'),\
     "risk_class": (57, 10, False, False, 'risk_class',    'char')\
    }                   

IN_CUSTOMER_EXTENSION_NOHIST_QUERY =\
   "SELECT " + TOP_N + "\
        INSTITUTE AS INSTITUTE,\
        CUSTNO AS CUSTNO,\
        CUST_SP_01  AS CUST_SP_01,\
        CUST_SP_02  AS CUST_SP_02,\
        CUST_SP_03  AS CUST_SP_03,\
        CUST_SP_04  AS CUST_SP_04,\
        CUST_SP_05  AS CUST_SP_05,\
        CUST_SP_06  AS CUST_SP_06,\
        CUST_SP_07  AS CUST_SP_07,\
        CUST_SP_08  AS CUST_SP_08,\
        CUST_SP_09  AS CUST_SP_09,\
        CUST_SP_10  AS CUST_SP_10,\
        CUST_SP_11  AS CUST_SP_11,\
        CUST_SP_12  AS CUST_SP_12,\
        CUST_SP_13  AS CUST_SP_13,\
        CUST_SP_14  AS CUST_SP_14,\
        CUST_SP_15  AS CUST_SP_15,\
        CUST_SP_16  AS CUST_SP_16,\
        CUST_SP_17  AS CUST_SP_17,\
        CUST_SP_18  AS CUST_SP_18,\
        CUST_SP_19  AS CUST_SP_19,\
        CUST_SP_20  AS CUST_SP_20,\
        CUST_SP_21  AS CUST_SP_21,\
        CUST_SP_22  AS CUST_SP_22,\
        CUST_SP_23  AS CUST_SP_23,\
        CUST_SP_24  AS CUST_SP_24,\
        CUST_SP_25  AS CUST_SP_25,\
        CUST_SP_26  AS CUST_SP_26,\
        CUST_SP_27  AS CUST_SP_27,\
        CUST_SP_28  AS CUST_SP_28,\
        CUST_SP_29  AS CUST_SP_29,\
        CUST_SP_30  AS CUST_SP_30,\
        CUST_SP_31  AS CUST_SP_31,\
        CUST_SP_32  AS CUST_SP_32,\
        CUST_SP_33  AS CUST_SP_33,\
        CUST_SP_34  AS CUST_SP_34,\
        CUST_SP_35  AS CUST_SP_35,\
        CUST_SP_36  AS CUST_SP_36,\
        CUST_SP_37  AS CUST_SP_37,\
        CUST_SP_38  AS CUST_SP_38,\
        CUST_SP_39  AS CUST_SP_39,\
        CUST_SP_40  AS CUST_SP_40,\
        CUST_SP_41  AS CUST_SP_41,\
        CUST_SP_42  AS CUST_SP_42,\
        CUST_SP_43  AS CUST_SP_43,\
        CUST_SP_44  AS CUST_SP_44,\
        ''  AS CUST_SP_45\
    FROM dbo.in_Customer_extension_nohist\
    WHERE 1=1\
    ORDER BY institute, custno"
     
# field_name: (Seq, Len, Mandatory, Key, Alias, DataType)     
IN_CUSTOMER_EXTENSION_NOHIST_ROW =\
    {"institute":  ( 1,  4,  True,  True,  'institute',  'char'),\
     "custno":     ( 2,  16, True,  True,  'custno',     'char'),\
     "cust_sp_01": ( 3,  64, False, False, 'cust_sp_01', 'char'),\
     "cust_sp_02": ( 4,  64, False, False, 'cust_sp_02', 'char'),\
     "cust_sp_03": ( 5,  64, False, False, 'cust_sp_03', 'char'),\
     "cust_sp_04": ( 6,  64, False, False, 'cust_sp_04', 'char'),\
     "cust_sp_05": ( 7,  64, False, False, 'cust_sp_05', 'char'),\
     "cust_sp_06": ( 8,  64, False, False, 'cust_sp_06', 'char'),\
     "cust_sp_07": ( 9,  64, False, False, 'cust_sp_07', 'char'),\
     "cust_sp_08": (10,  64, False, False, 'cust_sp_08', 'char'),\
     "cust_sp_09": (11,  64, False, False, 'cust_sp_09', 'char'),\
     "cust_sp_10": (12,  64, False, False, 'cust_sp_10', 'char'),\
     "cust_sp_11": (13,  64, False, False, 'cust_sp_11', 'char'),\
     "cust_sp_12": (14,  64, False, False, 'cust_sp_12', 'char'),\
     "cust_sp_13": (15,  64, False, False, 'cust_sp_13', 'char'),\
     "cust_sp_14": (16,  64, False, False, 'cust_sp_14', 'char'),\
     "cust_sp_15": (17,  64, False, False, 'cust_sp_15', 'char'),\
     "cust_sp_16": (18,  64, False, False, 'cust_sp_16', 'char'),\
     "cust_sp_17": (19,  64, False, False, 'cust_sp_17', 'char'),\
     "cust_sp_18": (20,  64, False, False, 'cust_sp_18', 'char'),\
     "cust_sp_19": (21,  64, False, False, 'cust_sp_19', 'char'),\
     "cust_sp_20": (22,  64, False, False, 'cust_sp_20', 'char'),\
     "cust_sp_21": (23,  64, False, False, 'cust_sp_21', 'char'),\
     "cust_sp_22": (24,  64, False, False, 'cust_sp_22', 'char'),\
     "cust_sp_23": (25,  64, False, False, 'cust_sp_23', 'char'),\
     "cust_sp_24": (26,  64, False, False, 'cust_sp_24', 'char'),\
     "cust_sp_25": (27,  64, False, False, 'cust_sp_25', 'char'),\
     "cust_sp_26": (28,  64, False, False, 'cust_sp_26', 'char'),\
     "cust_sp_27": (29,  64, False, False, 'cust_sp_27', 'char'),\
     "cust_sp_28": (30,  64, False, False, 'cust_sp_28', 'char'),\
     "cust_sp_29": (31,  64, False, False, 'cust_sp_29', 'char'),\
     "cust_sp_30": (32,  64, False, False, 'cust_sp_30', 'char'),\
     "cust_sp_31": (33,  64, False, False, 'cust_sp_31', 'char'),\
     "cust_sp_32": (34,  64, False, False, 'cust_sp_32', 'char'),\
     "cust_sp_33": (35,  64, False, False, 'cust_sp_33', 'char'),\
     "cust_sp_34": (36,  64, False, False, 'cust_sp_34', 'char'),\
     "cust_sp_35": (37,  64, False, False, 'cust_sp_35', 'char'),\
     "cust_sp_36": (38,  64, False, False, 'cust_sp_36', 'char'),\
     "cust_sp_37": (39,  64, False, False, 'cust_sp_37', 'char'),\
     "cust_sp_38": (40,  64, False, False, 'cust_sp_38', 'char'),\
     "cust_sp_39": (41,  64, False, False, 'cust_sp_39', 'char'),\
     "cust_sp_40": (42,  64, False, False, 'cust_sp_40', 'char'),\
     "cust_sp_41": (43,  64, False, False, 'cust_sp_41', 'char'),\
     "cust_sp_42": (44,  64, False, False, 'cust_sp_42', 'char'),\
     "cust_sp_43": (45,  64, False, False, 'cust_sp_43', 'char'),\
     "cust_sp_44": (45,  64, False, False, 'cust_sp_44', 'char'),\
     "cust_sp_45": (45,  64, False, False, 'cust_sp_45', 'char')\
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
   'SELECT ' + TOP_N + '\
     1 AS CUSTNO, ISNULL(COUNTRYINT,0) AS COUNTRYINT, COUNTRYEXT, "DESC"\
    FROM dbo.in_country\
    ORDER BY COUNTRYEXT'
    
IN_COUNTRY_ROW =\
    {"countryint":   (1,  3,  True,  True, 'countryint',    'char'),\
     "countryext":   (2,  3, True,  True,  'countryext',    'char'),\
     "desc":         (3,128, True,  True,  'desc',          'char')\
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
      STATUS,\
      TR_EMPLNO,\
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
     FROM dbo.in_transaction t\
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
     "status":           (25,  1, False, False, 'status',            'char'),\
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
     "analytical_trans_code":(53,  6, False, False, 'cshyn',         'char'),\
     "early_liquidation_flag":(54,  1, False, False, 'analytical_trans_code',  'char'),\
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
   "SELECT  " + TOP_N + "\
      t.cust_institute,\
      NVL(t.cust_custno, '') as custno,\
      NVL(t.acc_businesstype, '') as acc_businesstype,\
      NVL(t.acc_accno, '') as acc_accno ,\
      NVL(t.acc_businessno, '') as acc_businessno,\
      NVL(t.acc_currencyiso, '') as acc_currencyiso,\
      t.entrydate as entrydate,\
      t.valuedate as valuedate,\
      NVL(t.businessno_trans, '') as businessno_trans,\
      NVL(t.tr_sp_01, '') as tr_sp_01,\
      NVL(t.tr_sp_02, '') as tr_sp_02,\
      NVL(t.tr_sp_03, '') as tr_sp_03,\
      NVL(t.tr_sp_04, '') as tr_sp_04\
   FROM am.transaction_extension t\
    WHERE 1=1'\
    ORDER BY 1,2,3,4,5,6,7,8,9"

IN_TRANSACTION_EXTENSION_ROW =\
    {"cust_institute":   (1,  4,  True,  True,  'cust_institute',   'char'),\
     "cust_custno":      (2,  16, True,  True,  'custno',           'char'),\
     "acc_businesstype": (3,  64, False, False, 'acc_businesstype', 'char'),\
     "acc_accno":        (4,  64, False, False, 'acc_accno',        'char'),\
     "acc_businessno":   (5,  64, False, False, 'acc_businessno',   'char'),\
     "acc_currencyiso":  (6,  64, False, False, 'acc_currencyiso',  'char'),\
     "entrydate":        (7,  64, False, False, 'entrydate',        'char'),\
     "valuedate":        (8,  64, False, False, 'valuedate',        'char'),\
     "businessno_trans": (9,  64, False, False, 'businessno_trans', 'char'),\
     "tr_sp_01":         (10, 64, False, False, 'tr_sp_01',         'char'),\
     "tr_sp_02":         (11, 64, False, False, 'tr_sp_02',         'char'),\
     "tr_sp_03":         (12, 64, False, False, 'tr_sp_03',         'char'),\
     "tr_sp_04":         (13, 64, False, False, 'tr_sp_04',         'char'),\
     "tr_sp_05":         (14, 64, False, False, '',                 'char'),\
     "tr_sp_06":         (15, 64, False, False, '',                 'char'),\
     "tr_sp_07":         (16, 64, False, False, '',                 'char'),\
     "tr_sp_08":         (17, 64, False, False, '',                 'char'),\
     "tr_sp_09":         (18, 64, False, False, '',                 'char'),\
     "tr_sp_10":         (19, 64, False, False, '',                 'char'),\
    }


IN_ACCOUNT_QUERY =\
   "SELECT " + TOP_N + "\
     INSTITUTE,\
     CUST_CUSTNO AS CUSTNO,\
     CAST(BUSINESSTYPE AS NVARCHAR(4)) AS BUSINESSTYPE,\
     ACCNO,\
     BUSINESSNO,\
     ACC_CURRENCYISO,\
     ACCOPENING,\
     ACCCLOSE,\
	 ACCHOLD_INSTITUTE,\
	 ACCHOLDCUSTNO,\
	 ACCLIMIT,\
	 ACCBALANCE,\
	 SUM_CRED_RUN_YEAR AS SUMCREDRUNYEAR,\
	 SUM_DEB_RUN_YEAR as SUMDEBRUNYEAR,\
	 NUMBERACCOUNTS,\
	 ACC_FLAG_01 AS ACC_FLAG_01,\
	 PODTYPE,\
	 IBAN,\
	 ACC_TYPE,\
	 HOLD_MAIL,\
	 EMPLNO,\
	 PURPOSE\
   FROM dbo.in_Account\
    WHERE len(cust_custno) >= 1\
   ORDER BY 1,2,3,4,5,6"
   
# field_name: (Seq, Len, Mandatory, Key, Alias, DataType)     
IN_ACCOUNT_ROW =\
    {"cust_institute":    ( 1,  4,  True,  True,  'institute',   'char'),\
     "cust_custno":       ( 2,  16, True,  True,  'custno',           'char'),\
     "businesstype":      ( 3,   4, False, False, 'businesstype',     'char'),\
     "accno":             ( 4,  11, False, False, 'accno',            'char'),\
     "businessno":        ( 5,  11, False, False, 'businessno',       'char'),\
     "acc_currencyiso":   ( 6,   3, False, False, 'acc_currencyiso',  'char'),\
     "accopening":        ( 7,   8, False, False, 'accopening',       'date'),\
     "accclose":          ( 8,   8, False, False, 'accclose',         'date'),\
     "acchold_institute": ( 9,   4, False, False, 'acchold_institute','char'),\
     "acchold_custno":    (10,  16, False, False, 'accholdcustno',    'char'),\
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
     "hold_mail":         (43,   1, False, False, 'hold_mail',        'char'),\
     "emplno":            (44,  16, False, False, 'emplno',           'char'),\
     "purpose":           (45,  32, False, False, 'purpose',          'char')\
    }

IN_ACCOUNT_EXTENSION_NOHIST_QUERY =\
   "SELECT " + TOP_N + "\
     INSTITUTE,\
     CUSTNO AS CUSTNO,\
     BUSINESSTYPE,\
     ACCNO,\
     BUSINESSNO,\
     ACC_CURRENCYISO,\
     ACC_SP_01,\
     ACC_SP_02\
   FROM dbo.in_Account_extension_nohist\
   ORDER BY 1,2,3,4,5,6"

# field_name: (Seq, Len, Mandatory, Key, Alias, DataType)     
IN_ACCOUNT_EXTENSION_NOHIST_ROW =\
    {"institute":      ( 1,  4,  True,  True,  'institute', 'char'),\
     "cust_custno":    ( 2,  16, True,  True,  'custno',         'char'),\
     "businesstype":   ( 3,   4, False, False, 'businesstype',   'char'),\
     "accno":          ( 4,  11, False, False, 'accno',          'char'),\
     "businessno":     ( 5,  11, False, False, 'businessno',     'char'),\
     "acc_currencyiso":( 6,   3, False, False, 'acc_currencyiso','char'),\
     "acc_sp_01":      ( 7,  64, False, False, 'acc_sp_01',      'char'),\
     "acc_sp_02":      ( 8,  64, False, False, 'acc_sp_02',      'char')\
    }

  

IN_BENEFICIAL_OWNER_QUERY=\
   "SELECT " + TOP_N + "\
     '\"' + INSTITUTE + '\"' AS INSTITUTE,\
     '\"' + CUSTNO + '\"' AS CUSTNO,\
     '\"' + REL_CUSTNO + '\"' AS REL_CUSTNO,\
     '\"' + REL_TYPE + '\"' AS REL_TYPE,\
     ISNULL(CONVERT(varchar, REL_SHARE), '0') + '.0' AS REL_SHARE,\
     '\"' + REL_FLAG1 + '\"' AS REL_FLAG1,\
     '\"' + REL_FLAG2 + '\"' AS REL_FLAG2,\
     '\"' + REL_FLAG3 + '\"' AS REL_FLAG3,\
     '\"' + REL_COMMENT + '\"' AS REL_COMMENT,\
     '\"' + PROCESSFLAG + '\"' AS PROCESSFLAG\
   FROM dbo.in_Beneficial_owner\
   ORDER BY 1,2,3,4"

# field_name: (Seq, Len, Mandatory, Key, Alias, DataType)     
IN_BENEFICIAL_OWNER_ROW =\
    {"institute":      ( 1,   6, True,  True,  'institute',  'char'),\
     "cust_custno":    ( 2,  18, True,  True,  'custno',     'char'),\
     "rel_custno":     ( 3,  18, False, False, 'rel_custno', 'char'),\
     "rel_type":       ( 4,   4, False, False, 'rel_type',   'char'),\
     "rel_share":      ( 5,   9, False, False, 'rel_share',  'char'),\
     "rel_flag1":      ( 6,   3, False, False, 'rel_flag1',  'char'),\
     "rel_flag2":      ( 7,   3, False, False, 'rel_flag2',  'char'),\
     "rel_flag3":      ( 8,   3, False, False, 'rel_flag3',  'char'),\
     "rel_comment":    ( 9,1154, False, False, 'rel_comment','char'),\
     "processflag":    (10,   3, False, False, 'processflag','char')
    }

IN_POWER_OF_DISPOSAL_QUERY =\
   "SELECT " + TOP_N + "\
     INSTITUTE,\
     CUST_CUSTNO AS CUSTNO,\
     BUSINESSTYPE,\
     ACCNO,\
     BUSINESSNO,\
     ACC_CURRENCYISO,\
     POD_CUSTNO,\
     PODTYPE,\
     CHECK_YN\
    FROM dbo.in_power_of_disposal\
   ORDER BY 1,2,3,4,5,6,7"

IN_POWER_OF_DISPOSAL_ROW =\
    {"cust_institute": ( 1,  4,  True,  True,  'institute', 'char'),\
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
     "check_yn":       (33,   1, False, False, 'check_yn',       'char')\
    }


IN_CUST_SERV_MANAGER_QUERY =\
   "SELECT " + TOP_N + "\
     0 as CUSTNO,\
     INSTITUTE,\
     CSMNO,\
     CSMNAME,\
	 CSMMAIL\
    FROM dbo.in_cust_serv_manager\
   ORDER BY institute, csmno"
   
IN_CUST_SERV_MANAGER_ROW =\
    {"institute": ( 1,  4,  True,  True, 'institute',  'char'),\
     "csmno":     ( 2, 12, True,  True,  'csmno',       'char'),\
     "csmname":   ( 3, 32, False, False, 'csmname',     'char'),\
     "csmmail":   ( 3, 64, False, False, 'csmmail',     'char'),\
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