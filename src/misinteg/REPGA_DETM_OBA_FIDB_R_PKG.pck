CREATE OR REPLACE PACKAGE REPGA_DETM_OBA_FIDB_R_PKG IS
	-- %version   0.2   2012.12.10.
	-- %version   0.1   2012.10.18.
	-- %author   Dörnyei László
	-- %usage loading package for detail area
	-- %intab tablelist:  KMDW.MI_RB_PROD_DEFAULT_MTH<BR/>
	--                    KMDW.MI_RB_ACCT_MTH<BR/>
	--                    KMDW.MI_RB_RESTRAINTS_MTH<BR/>
	--                    KMDW.MI_RB_JOINT_ACCT_MTH<BR/>
	--                    KMDW.MI_FM_CLIENT_MTH<BR/>
	--                    KMDW.MI_FM_CLIENT_INDVL_MTH<BR/>
	--                    KMDW.MI_FM_TAX_RATE_MTH<BR/>
	--                    KMDW.MI_FM_CLIENT_MTH<BR/>
	--                    EBHU.AGA_ACCOUNT2<BR/>
	--                    REPGA_STAM_RB_BAL<BR/>

	-- %outtab tablelist: REPGA_DETM_OBA_FIDB_R

	PROCEDURE MAIN(P_SYM_RUN_DATE DATE DEFAULT EBH_PWM_PUBLIC_P.F_GETSYMRUNDATE);

END;
/
CREATE OR REPLACE PACKAGE BODY REPGA_DETM_OBA_FIDB_R_PKG IS
	-- %version   0.2   2012.12.10.
	-- %version   0.1   2012.10.18.
	-- %author   Dörnyei László
	--
	-- Procedure for calculating and loading data into REPGA_DETM_OBA_FIDB_R table
	-- %param p_sym_run_data: symbols running time <br>
	--              in form of date type
	-- %raises NO
	-- %dynasql YES
	-- %autonom NO
	-- %dbmsout NO
	----------------------------------------------------------------------------------
	-- %version   0.2   DL ERSTEOBA-3 javítópatch appname REPGA_DETM_OBA_FIDB_R_PKG, forrás REPGA_DETM_DEALS_EXP
	-- %version   0.1   DL ERSTEOBA-3 INITIAL

	GC_APPNAME CONSTANT VARCHAR2(30) := 'REPGA_DETM_OBA_FIDB_R_PKG';

	GC_TARGETTABLE CONSTANT VARCHAR2(30) := 'REPGA_DETM_OBA_FIDB_R';

	G_PROCESSED NUMBER := 0;

	G_RUNID NUMBER;

	G_COMMITPOINT NUMBER := 200000;

	G_BATCHSIZE NUMBER := 25000;
	----------------------------------------
	PROCEDURE INSERT_TMPM_OBA_TI(P_SYM_RUN_DATE DATE) IS
		C_ACCTION CONSTANT VARCHAR2(30) := 'INSERT_TMPM_OBA_TI';
		L_ROWNUMBER NUMBER := 0;
		L_INDEX     NUMBER := 0;
		L_LAST      NUMBER;

		TYPE T_ACCT_TYPE IS TABLE OF REPGA_TMPM_OBA_TI.ACCT_TYPE%TYPE INDEX BY PLS_INTEGER;
		TYPE T_TAXABLE_IND IS TABLE OF REPGA_TMPM_OBA_TI.TAXABLE_IND%TYPE INDEX BY PLS_INTEGER;

		L_ACCT_TYPE   T_ACCT_TYPE;
		L_TAXABLE_IND T_TAXABLE_IND;

		CURSOR HC IS(
			SELECT ACCT_TYPE,
						 NVL(TAXABLE_IND,
								 'N') TAXABLE_IND
				FROM MI_RB_PROD_DEFAULT_MTH
			 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE);
	BEGIN
		REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
		OPEN HC;
		LOOP
			REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION || ' ' || TO_CHAR(L_ROWNUMBER));
			L_ACCT_TYPE.DELETE;
			FETCH HC BULK COLLECT
				INTO L_ACCT_TYPE, L_TAXABLE_IND LIMIT G_BATCHSIZE;
			L_LAST := L_ACCT_TYPE.COUNT;
			EXIT WHEN L_LAST = 0;
			L_ROWNUMBER := L_ROWNUMBER + L_LAST;

			FORALL X IN 1 .. L_LAST
				INSERT INTO REPGA_TMPM_OBA_TI
					(ACCT_TYPE,
					 TAXABLE_IND)
				VALUES
					(L_ACCT_TYPE(X),
					 L_TAXABLE_IND(X));
			IF L_INDEX >= G_COMMITPOINT THEN
				COMMIT;
				L_INDEX := 0;
			ELSE
				L_INDEX := L_INDEX + L_LAST;
			END IF;
			COMMIT;
		END LOOP;
		CLOSE HC;
	END;
	----------------------------------------
	PROCEDURE INSERT_TMPM_OBA_TAX_RATE(P_SYM_RUN_DATE DATE) IS
		C_ACCTION CONSTANT VARCHAR2(30) := 'INSERT_TMPM_OBA_TAX_RATE';
		L_ROWNUMBER NUMBER := 0;
		L_INDEX     NUMBER := 0;
		L_LAST      NUMBER;

		TYPE T_COUNTRY IS TABLE OF REPGA_TMPM_OBA_TAX_RATE.COUNTRY%TYPE INDEX BY PLS_INTEGER;
		TYPE T_TAX_RATE IS TABLE OF REPGA_TMPM_OBA_TAX_RATE.TAX_RATE%TYPE INDEX BY PLS_INTEGER;

		L_COUNTRY  T_COUNTRY;
		L_TAX_RATE T_TAX_RATE;

		CURSOR HC IS(
			SELECT COUNTRY,
						 TAX_RATE / 100 AS TAX_RATE
				FROM KMDW.MI_FM_TAX_RATE_MTH
			 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE);
	BEGIN
		REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
		OPEN HC;
		LOOP
			REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION || ' ' || TO_CHAR(L_ROWNUMBER));
			L_COUNTRY.DELETE;
			FETCH HC BULK COLLECT
				INTO L_COUNTRY, L_TAX_RATE LIMIT G_BATCHSIZE;
			L_LAST := L_COUNTRY.COUNT;
			EXIT WHEN L_LAST = 0;
			L_ROWNUMBER := L_ROWNUMBER + L_LAST;

			FORALL X IN 1 .. L_LAST
				INSERT INTO REPGA_TMPM_OBA_TAX_RATE
					(COUNTRY,
					 TAX_RATE)
				VALUES
					(L_COUNTRY(X),
					 L_TAX_RATE(X));
			IF L_INDEX >= G_COMMITPOINT THEN
				COMMIT;
				L_INDEX := 0;
			ELSE
				L_INDEX := L_INDEX + L_LAST;
			END IF;
			COMMIT;
		END LOOP;
		CLOSE HC;
	END;
	----------------------------------------
	PROCEDURE INSERT_TMPM_OBA_TAX(P_SYM_RUN_DATE DATE) IS
		C_ACCTION CONSTANT VARCHAR2(30) := 'INSERT_TMPM_OBA_TAX';
		L_ROWNUMBER NUMBER := 0;
		L_INDEX     NUMBER := 0;
		L_LAST      NUMBER;

		TYPE T_CLIENT_NO IS TABLE OF REPGA_TMPM_OBA_TAX.CLIENT_NO%TYPE INDEX BY PLS_INTEGER;
		TYPE T_CLIENT_CAT IS TABLE OF REPGA_TMPM_OBA_TAX.CLIENT_CAT%TYPE INDEX BY PLS_INTEGER;
		TYPE T_TAX_HANDLING_IND IS TABLE OF REPGA_TMPM_OBA_TAX.TAX_HANDLING_IND%TYPE INDEX BY PLS_INTEGER;
		TYPE T_COUNTRY_LOC IS TABLE OF REPGA_TMPM_OBA_TAX.COUNTRY_LOC%TYPE INDEX BY PLS_INTEGER;
		TYPE T_TAX_HANDLING_IND2 IS TABLE OF REPGA_TMPM_OBA_TAX.TAX_HANDLING_IND2%TYPE INDEX BY PLS_INTEGER;

		L_CLIENT_NO         T_CLIENT_NO;
		L_CLIENT_CAT        T_CLIENT_CAT;
		L_TAX_HANDLING_IND  T_TAX_HANDLING_IND;
		L_COUNTRY_LOC       T_COUNTRY_LOC;
		L_TAX_HANDLING_IND2 T_TAX_HANDLING_IND2;

		CURSOR HC IS(
			SELECT CLIENT_NO,
						 COALESCE(CASE
												WHEN MAJOR_CATEGORY = 'PR' AND PROFIT_SEGMENT = '0100' THEN
												 'M'
												WHEN MAJOR_CATEGORY != 'PR' AND PROFIT_SEGMENT = '0100' THEN
												 'C'
												WHEN MAJOR_CATEGORY = 'PR' AND PROFIT_SEGMENT = '0120' THEN
												 'M'
												WHEN PROFIT_SEGMENT LIKE '03%' THEN
												 'C'
											END,
											DECODE(MAJOR_CATEGORY,
														 'PR',
														 'M',
														 'C')) CLIENT_CAT,
						 TAX_HANDLING_IND, -- 1st
						 COUNTRY_LOC,
						 TAX_HANDLING_IND2 -- case Y > rb_prod_default
				FROM MI_FM_CLIENT_MTH
			 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
				 AND (TAX_HANDLING_IND = 'Y' OR TAX_HANDLING_IND2 = 'Y')
				 AND INTERNAL_IND != 'Y'
				 AND MAJOR_CATEGORY != 'CI');
	BEGIN
		REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
		OPEN HC;
		LOOP
			REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION || ' ' || TO_CHAR(L_ROWNUMBER));
			L_CLIENT_NO.DELETE;
			FETCH HC BULK COLLECT
				INTO L_CLIENT_NO, L_CLIENT_CAT, L_TAX_HANDLING_IND, L_COUNTRY_LOC, L_TAX_HANDLING_IND2 LIMIT G_BATCHSIZE;
			L_LAST := L_CLIENT_NO.COUNT;
			EXIT WHEN L_LAST = 0;
			L_ROWNUMBER := L_ROWNUMBER + L_LAST;

			FORALL X IN 1 .. L_LAST
				INSERT INTO REPGA_TMPM_OBA_TAX
					(CLIENT_NO,
					 CLIENT_CAT,
					 TAX_HANDLING_IND,
					 COUNTRY_LOC,
					 TAX_HANDLING_IND2)
				VALUES
					(L_CLIENT_NO(X),
					 L_CLIENT_CAT(X),
					 L_TAX_HANDLING_IND(X),
					 L_COUNTRY_LOC(X),
					 L_TAX_HANDLING_IND2(X));
			IF L_INDEX >= G_COMMITPOINT THEN
				COMMIT;
				L_INDEX := 0;
			ELSE
				L_INDEX := L_INDEX + L_LAST;
			END IF;
			COMMIT;
		END LOOP;
		CLOSE HC;
	END;
	----------------------------------------
	PROCEDURE INSERT_TMPM_OBA_AGA(P_SYM_RUN_DATE DATE) IS
		C_ACCTION CONSTANT VARCHAR2(30) := 'INSERT_TMPM_OBA_AGA';
		L_ROWNUMBER NUMBER := 0;
		L_INDEX     NUMBER := 0;
		L_LAST      NUMBER;

		TYPE T_ACCT_NO IS TABLE OF REPGA_TMPM_OBA_AGA.ACCT_NO%TYPE INDEX BY PLS_INTEGER;
		TYPE T_CLIENT_NO IS TABLE OF REPGA_TMPM_OBA_AGA.CLIENT_NO%TYPE INDEX BY PLS_INTEGER;

		L_ACCT_NO   T_ACCT_NO;
		L_CLIENT_NO T_CLIENT_NO;

		CURSOR HC IS(
			SELECT ACCT_NO,
						 CLIENT_NO
				FROM (SELECT ACCT_NO,
										 (SELECT CLIENT_NO
												FROM KMDW.MI_RB_ACCT_MTH
											 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
												 AND ACCT_NO = X.ACCT_NO
												 AND ACCT_STATUS != 'C') CLIENT_NO
								FROM AGA_ACCOUNT2@EBHP_EBH_REPGA.ERSTE.HU X)
			 WHERE CLIENT_NO IS NOT NULL);
	BEGIN
		REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
		OPEN HC;
		LOOP
			REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION || ' ' || TO_CHAR(L_ROWNUMBER));
			L_ACCT_NO.DELETE;
			FETCH HC BULK COLLECT
				INTO L_ACCT_NO, L_CLIENT_NO LIMIT G_BATCHSIZE;
			L_LAST := L_ACCT_NO.COUNT;
			EXIT WHEN L_LAST = 0;
			L_ROWNUMBER := L_ROWNUMBER + L_LAST;

			FORALL X IN 1 .. L_LAST
				INSERT INTO REPGA_TMPM_OBA_AGA
					(ACCT_NO,
					 CLIENT_NO)
				VALUES
					(L_ACCT_NO(X),
					 L_CLIENT_NO(X));
			IF L_INDEX >= G_COMMITPOINT THEN
				COMMIT;
				L_INDEX := 0;
			ELSE
				L_INDEX := L_INDEX + L_LAST;
			END IF;
			COMMIT;
		END LOOP;
		CLOSE HC;
	END;
	----------------------------------------
	PROCEDURE INSERT_TMPM_OBA_FIDB(P_SYM_RUN_DATE DATE) IS
		C_ACCTION CONSTANT VARCHAR2(30) := 'INSERT_TMPM_OBA_FIDB';
		L_ROWNUMBER NUMBER := 0;
		L_INDEX     NUMBER := 0;
		L_LAST      NUMBER;

		TYPE T_CLIENT_NO IS TABLE OF REPGA_TMPM_OBA_FIDB.CLIENT_NO%TYPE INDEX BY PLS_INTEGER;
		TYPE T_ACCOUNT_NO IS TABLE OF REPGA_TMPM_OBA_FIDB.ACCOUNT_NO%TYPE INDEX BY PLS_INTEGER;
		TYPE T_SAP_FLAG IS TABLE OF REPGA_TMPM_OBA_FIDB.SAP_FLAG%TYPE INDEX BY PLS_INTEGER;
		TYPE T_SAP_AMOUNT IS TABLE OF REPGA_TMPM_OBA_FIDB.SAP_AMOUNT%TYPE INDEX BY PLS_INTEGER;
		TYPE T_CCY IS TABLE OF REPGA_TMPM_OBA_FIDB.CCY%TYPE INDEX BY PLS_INTEGER;
		TYPE T_AMOUNT_HUF IS TABLE OF REPGA_TMPM_OBA_FIDB.AMOUNT_HUF%TYPE INDEX BY PLS_INTEGER;

		L_CLIENT_NO  T_CLIENT_NO;
		L_ACCOUNT_NO T_ACCOUNT_NO;
		L_SAP_FLAG   T_SAP_FLAG;
		L_SAP_AMOUNT T_SAP_AMOUNT;
		L_CCY        T_CCY;
		L_AMOUNT_HUF T_AMOUNT_HUF;

		CURSOR HC IS(
		/* v0.2 repga_stam_ex_bal_v helyett repga_detm_deals_exp-re átültetve
          SELECT CLIENT_NO,
                 ACCOUNT_NO, --sys_id, modul_id, sap_system_code,
                 CASE
                   WHEN SAP_FOKONYV IN
                        (
                         -- balance
                         --'H441100','H441200','H441300','H442100','H442200','H442300',
                         --'H443100','H443200','H443300','H443500',
                         --'H444100','H444200','H444300','H444400'
                         SELECT DATA_1
                           FROM REPGA_INT_MAPPING_DATA T
                          WHERE MAPPING_ID = (SELECT MAPPING_ID
                                                FROM REPGA_INT_MAPPINGS
                                               WHERE MAPPING_SHORT_NAME = 'OBA_DEPO_BAL')
                            AND RETURN_VALUE = 'Y'
                            AND SYSTIMESTAMP BETWEEN FROM_DATE AND TO_DATE) THEN
                    'B'
                   ELSE
                    'I'
                 END SAP_FLAG,
                 -SAP_AMOUNT SAP_AMOUNT,
                 SAP_CCY CCY,
                 --(
                 --SELECT ROUND(-v.sap_amount*central_bank_rate,0)
                 --FROM    OBA_ARF
                 --WHERE  ccy = v.sap_ccy
                 --) amount_huf
                 KMDW.CONV_AMT_MTH(P_SYM_RUN_DATE,
                                   V.SAP_CCY,
                                   -V.SAP_AMOUNT,
                                   'HUF') AS AMOUNT_HUF
            FROM REPGA_STAM_EX_BAL_V V
           WHERE SYM_RUN_DATE = P_SYM_RUN_DATE --mth_date = (SELECT TRUNC(actdat,'MONTH') FROM OBA_DATE)
             AND SAP_VALUE_DATE = P_SYM_RUN_DATE --(SELECT actdat FROM OBA_DATE)
                --AND    client_no = 'T22287'
             AND SAP_FOKONYV IN
                 (
                  -- new version
                  -- balance
                  --'H441100','H441200','H441300','H442100','H442200','H442300',
                  --'H443100','H443200','H443300','H443500',
                  --'H444100','H444200','H444300','H444400',
                  SELECT DATA_1
                    FROM REPGA_INT_MAPPING_DATA T
                   WHERE MAPPING_ID = (SELECT MAPPING_ID
                                         FROM REPGA_INT_MAPPINGS
                                        WHERE MAPPING_SHORT_NAME = 'OBA_DEPO_BAL')
                     AND RETURN_VALUE = 'Y'
                     AND SYSTIMESTAMP BETWEEN FROM_DATE AND TO_DATE
                  UNION
                  -- interest
                  --'H492100','H492200','H492600','H492700',
                  --'H493110','H495100','H495200'
                  SELECT DATA_1
                    FROM REPGA_INT_MAPPING_DATA T
                   WHERE MAPPING_ID = (SELECT MAPPING_ID
                                         FROM REPGA_INT_MAPPINGS
                                        WHERE MAPPING_SHORT_NAME = 'OBA_DEPO_INT')
                     AND RETURN_VALUE = 'Y'
                     AND SYSTIMESTAMP BETWEEN FROM_DATE AND TO_DATE)
             AND EXISTS
           (
                  --SELECT 1
                  --FROM   OBA_CCY
                  --WHERE  ccy = v.sap_ccy
                  SELECT 1
                    FROM REPGA_INT_MAPPING_DATA T
                   WHERE MAPPING_ID =
                         (SELECT MAPPING_ID FROM REPGA_INT_MAPPINGS WHERE MAPPING_SHORT_NAME = 'OBA_CCY')
                     AND RETURN_VALUE = 'Y'
                     AND SYSTIMESTAMP BETWEEN FROM_DATE AND TO_DATE
                     AND DATA_1 = V.SAP_CCY)
             AND NOT EXISTS (SELECT 1 FROM REPGA_TMPM_OBA_AGA WHERE ACCT_NO = V.ACCOUNT_NO)
             AND TRIM(SAP_CUSTOMER) IN ('EM', 'LE', 'PC', 'PR', 'SC', 'AC', 'EM',
                  'NPSB', 'NPSC', 'NPSM', 'NPSP', 'OS')
             AND SAP_AMOUNT != 0
                -- SY-RB
             AND SAP_SYSTEM_CODE = '01'
             AND SYS_ID = '01'
             AND MODUL_ID = 'RB'*/
			SELECT CLIENT_NO,
						 ACCOUNT_NO, --sys_id, modul_id, sap_system_code,
						 CASE
							 WHEN LEDGER_ACCOUNT_SL_CODE IN
										(
										 -- balance
										 --'H441100','H441200','H441300','H442100','H442200','H442300',
										 --'H443100','H443200','H443300','H443500',
										 --'H444100','H444200','H444300','H444400'
										 SELECT DATA_1
											 FROM EBH_REPGA.REPGA_INT_MAPPING_DATA T
											WHERE MAPPING_ID = (SELECT MAPPING_ID
																						FROM EBH_REPGA.REPGA_INT_MAPPINGS
																					 WHERE MAPPING_SHORT_NAME = 'OBA_DEPO_BAL')
												AND RETURN_VALUE = 'Y'
												AND SYSTIMESTAMP BETWEEN FROM_DATE AND TO_DATE) THEN
								'B'
							 ELSE
								'I'
						 END SAP_FLAG,
						 -1 * AMOUNT SAP_AMOUNT,
						 CURRENCY_SL_CODE CCY,
						 -1 * AMOUNT_LCY AS AMOUNT_HUF
				FROM REPGA_DETM_DEALS_EXP V --REPGA_STAM_EX_BAL_V V
			 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE --mth_date = (SELECT TRUNC(actdat,'MONTH') FROM OBA_DATE)
						--AND SAP_VALUE_DATE = P_SYM_RUN_DATE --(SELECT actdat FROM OBA_DATE)
						--AND    client_no = 'T22287'
				 AND LEDGER_ACCOUNT_SL_CODE IN
						 (
							-- new version
							-- balance
							--'H441100','H441200','H441300','H442100','H442200','H442300',
							--'H443100','H443200','H443300','H443500',
							--'H444100','H444200','H444300','H444400',
							SELECT DATA_1
								FROM REPGA_INT_MAPPING_DATA T
							 WHERE MAPPING_ID = (SELECT MAPPING_ID
																		 FROM REPGA_INT_MAPPINGS
																		WHERE MAPPING_SHORT_NAME = 'OBA_DEPO_BAL')
								 AND RETURN_VALUE = 'Y'
								 AND SYSTIMESTAMP BETWEEN FROM_DATE AND TO_DATE
							UNION
							-- interest
							--'H492100','H492200','H492600','H492700',
							--'H493110','H495100','H495200'
							SELECT DATA_1
								FROM REPGA_INT_MAPPING_DATA T
							 WHERE MAPPING_ID = (SELECT MAPPING_ID
																		 FROM REPGA_INT_MAPPINGS
																		WHERE MAPPING_SHORT_NAME = 'OBA_DEPO_INT')
								 AND RETURN_VALUE = 'Y'
								 AND SYSTIMESTAMP BETWEEN FROM_DATE AND TO_DATE)
				 AND EXISTS
			 (
							--SELECT 1
							--FROM   OBA_CCY
							--WHERE  ccy = v.sap_ccy
							SELECT 1
								FROM REPGA_INT_MAPPING_DATA T
							 WHERE MAPPING_ID =
										 (SELECT MAPPING_ID FROM REPGA_INT_MAPPINGS WHERE MAPPING_SHORT_NAME = 'OBA_CCY')
								 AND RETURN_VALUE = 'Y'
								 AND SYSTIMESTAMP BETWEEN FROM_DATE AND TO_DATE
								 AND DATA_1 = V.CURRENCY_SL_CODE)
				 AND NOT EXISTS (SELECT 1 FROM REPGA_TMPM_OBA_AGA WHERE ACCT_NO = V.ACCOUNT_NO)
				 AND TRIM(CUSTOMER_TYPE_SL_CODE) IN ('EM', 'LE', 'PC', 'PR', 'SC', 'AC', 'EM',
							'NPSB', 'NPSC', 'NPSM', 'NPSP', 'OS')
				 AND AMOUNT != 0
						-- SY-RB
						--AND SAP_SYSTEM_CODE = '01'
						--AND SYS_ID = '01'
				 AND MODUL_ID = 'RB');
	BEGIN
		REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
		OPEN HC;
		LOOP
			REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION || ' ' || TO_CHAR(L_ROWNUMBER));
			L_ACCOUNT_NO.DELETE;
			FETCH HC BULK COLLECT
				INTO L_CLIENT_NO, L_ACCOUNT_NO, L_SAP_FLAG, L_SAP_AMOUNT, L_CCY, L_AMOUNT_HUF LIMIT G_BATCHSIZE;
			L_LAST := L_ACCOUNT_NO.COUNT;
			EXIT WHEN L_LAST = 0;
			L_ROWNUMBER := L_ROWNUMBER + L_LAST;

			FORALL X IN 1 .. L_LAST
				INSERT INTO REPGA_TMPM_OBA_FIDB
					(CLIENT_NO,
					 ACCOUNT_NO,
					 SAP_FLAG,
					 SAP_AMOUNT,
					 CCY,
					 AMOUNT_HUF)
				VALUES
					(L_CLIENT_NO(X),
					 L_ACCOUNT_NO(X),
					 L_SAP_FLAG(X),
					 L_SAP_AMOUNT(X),
					 L_CCY(X),
					 L_AMOUNT_HUF(X));
			IF L_INDEX >= G_COMMITPOINT THEN
				COMMIT;
				L_INDEX := 0;
			ELSE
				L_INDEX := L_INDEX + L_LAST;
			END IF;
			COMMIT;
		END LOOP;
		CLOSE HC;
	END;
	----------------------------------------
	PROCEDURE DELETE_TABLE(P_SYM_RUN_DATE DATE) IS
		C_ACCTION CONSTANT VARCHAR2(6) := 'DELETE';
	BEGIN
		REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
		REPGA_UTIL_PKG.DELETE_TABLE(GC_TARGETTABLE,
																P_SYM_RUN_DATE);
	END;
	----------------------------------------
	PROCEDURE INSERT_TABLE(P_SYM_RUN_DATE DATE) IS
		C_ACCTION CONSTANT VARCHAR2(6) := 'INSERT';
		L_ROWNUMBER NUMBER := 0;
		L_INDEX     NUMBER := 0;
		L_LAST      NUMBER;
		V_COMMIT    NUMBER := 0;

		TYPE T_SYM_RUN_DATE IS TABLE OF REPGA_DETM_OBA_FIDB_R.SYM_RUN_DATE%TYPE INDEX BY PLS_INTEGER;
		TYPE T_CLIENT_NO IS TABLE OF REPGA_DETM_OBA_FIDB_R.CLIENT_NO%TYPE INDEX BY PLS_INTEGER;
		TYPE T_ACCOUNT_NO IS TABLE OF REPGA_DETM_OBA_FIDB_R.ACCOUNT_NO%TYPE INDEX BY PLS_INTEGER;
		TYPE T_CCY IS TABLE OF REPGA_DETM_OBA_FIDB_R.CCY%TYPE INDEX BY PLS_INTEGER;
		TYPE T_BAL_AMT IS TABLE OF REPGA_DETM_OBA_FIDB_R.BAL_AMT%TYPE INDEX BY PLS_INTEGER;
		TYPE T_BAL_HUF IS TABLE OF REPGA_DETM_OBA_FIDB_R.BAL_HUF%TYPE INDEX BY PLS_INTEGER;
		TYPE T_INT_AMT IS TABLE OF REPGA_DETM_OBA_FIDB_R.INT_AMT%TYPE INDEX BY PLS_INTEGER;
		TYPE T_INT_HUF IS TABLE OF REPGA_DETM_OBA_FIDB_R.INT_HUF%TYPE INDEX BY PLS_INTEGER;
		TYPE T_TAX_FLAG IS TABLE OF REPGA_DETM_OBA_FIDB_R.TAX_FLAG%TYPE INDEX BY PLS_INTEGER;
		TYPE T_TIPUS IS TABLE OF REPGA_DETM_OBA_FIDB_R.TIPUS%TYPE INDEX BY PLS_INTEGER;
		TYPE T_IK IS TABLE OF REPGA_DETM_OBA_FIDB_R.IK%TYPE INDEX BY PLS_INTEGER;
		TYPE T_UL IS TABLE OF REPGA_DETM_OBA_FIDB_R.UL%TYPE INDEX BY PLS_INTEGER;
		TYPE T_DOLG IS TABLE OF REPGA_DETM_OBA_FIDB_R.DOLG%TYPE INDEX BY PLS_INTEGER;
		TYPE T_PDOLG IS TABLE OF REPGA_DETM_OBA_FIDB_R.PDOLG%TYPE INDEX BY PLS_INTEGER;
		TYPE T_ING IS TABLE OF REPGA_DETM_OBA_FIDB_R.ING%TYPE INDEX BY PLS_INTEGER;
		TYPE T_TAX_RATE IS TABLE OF REPGA_DETM_OBA_FIDB_R.TAX_RATE%TYPE INDEX BY PLS_INTEGER;
		TYPE T_INT_TAX IS TABLE OF REPGA_DETM_OBA_FIDB_R.INT_TAX%TYPE INDEX BY PLS_INTEGER;
		TYPE T_RESTRAINT IS TABLE OF REPGA_DETM_OBA_FIDB_R.RESTRAINT%TYPE INDEX BY PLS_INTEGER;
		TYPE T_RES_INT IS TABLE OF REPGA_DETM_OBA_FIDB_R.RES_INT%TYPE INDEX BY PLS_INTEGER;
		TYPE T_RES_TAX IS TABLE OF REPGA_DETM_OBA_FIDB_R.RES_TAX%TYPE INDEX BY PLS_INTEGER;
		TYPE T_RELX IS TABLE OF REPGA_DETM_OBA_FIDB_R.RELX%TYPE INDEX BY PLS_INTEGER;
		--TYPE T_RUN_ID IS TABLE OF REPGA_DETM_OBA_FIDB_R.RUN_ID%TYPE INDEX BY PLS_INTEGER;

		L_SYM_RUN_DATE T_SYM_RUN_DATE;
		L_CLIENT_NO    T_CLIENT_NO;
		L_ACCOUNT_NO   T_ACCOUNT_NO;
		L_CCY          T_CCY;
		L_BAL_AMT      T_BAL_AMT;
		L_BAL_HUF      T_BAL_HUF;
		L_INT_AMT      T_INT_AMT;
		L_INT_HUF      T_INT_HUF;
		L_TAX_FLAG     T_TAX_FLAG;
		L_TIPUS        T_TIPUS;
		L_IK           T_IK;
		L_UL           T_UL;
		L_DOLG         T_DOLG;
		L_PDOLG        T_PDOLG;
		L_ING          T_ING;
		L_TAX_RATE     T_TAX_RATE;
		L_INT_TAX      T_INT_TAX;
		L_RESTRAINT    T_RESTRAINT;
		L_RES_INT      T_RES_INT;
		L_RES_TAX      T_RES_TAX;
		L_RELX         T_RELX;
		--L_RUN_ID  T_RUN_ID;

		CURSOR HC IS(
			SELECT CLIENT_NO,
						 ACCOUNT_NO,
						 CCY,
						 BAL_AMT,
						 BAL_HUF,
						 INT_AMT,
						 INT_HUF,
						 COALESCE((SELECT (SELECT TAXABLE_IND
																FROM REPGA_TMPM_OBA_TI
															 WHERE ACCT_TYPE = RAM.ACCT_TYPE
																 AND ROWNUM = 1)
												FROM KMDW.MI_RB_ACCT_MTH RAM
											 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE --(SELECT actdat FROM OBA_DATE)
												 AND ACCT_NO = X.ACCOUNT_NO
												 AND EXISTS (SELECT 1
																FROM REPGA_TMPM_OBA_TAX
															 WHERE CLIENT_NO = X.CLIENT_NO
																 AND TAX_HANDLING_IND = 'Y'
																 AND TAX_HANDLING_IND2 = 'Y'

															)
												 AND ROWNUM = 1),
											(SELECT 'Y'
												 FROM REPGA_TMPM_OBA_TAX
												WHERE CLIENT_NO = X.CLIENT_NO
													AND TAX_HANDLING_IND = 'Y')) TAX_FLAG,
						 (SELECT DECODE(DEPOSIT_TYPE,
														'C',
														'F',
														'B')
								FROM KMDW.MI_RB_ACCT_MTH
							 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE --(SELECT actdat FROM OBA_DATE)
								 AND ACCT_NO = X.ACCOUNT_NO) TIPUS,
						 (SELECT INTERNAL_KEY
								FROM KMDW.MI_RB_ACCT_MTH
							 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE --(SELECT actdat FROM OBA_DATE)
								 AND ACCT_NO = X.ACCOUNT_NO) IK,
						 (SELECT 'Y'
								FROM KMDW.MI_RB_ACCT_MTH
							 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE --(SELECT actdat FROM OBA_DATE)
								 AND ACCT_NO = X.ACCOUNT_NO
								 AND ACCT_TYPE IN ('VLA', 'VLN', 'VL7', 'VL8')
								 AND ROWNUM = 1) UL, -- ugyvedi letet
						 COALESCE((SELECT DECODE(REPORTING,
																		'EM',
																		'Y',
																		NULL)
												FROM KMDW.MI_FM_CLIENT_MTH
											 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE --(SELECT actdat FROM OBA_DATE)
												 AND CLIENT_NO = X.CLIENT_NO
												 AND ROWNUM = 1),
											(SELECT 'Y'
												 FROM KMDW.MI_RB_ACCT_MTH
												WHERE SYM_RUN_DATE = P_SYM_RUN_DATE --(SELECT actdat FROM OBA_DATE)
													AND CLIENT_NO = X.CLIENT_NO
													AND ACCT_TYPE IN ('DFN', 'DF7', 'DFP', 'DFM', 'MF2')
													AND ACCT_STATUS != 'C'
													AND ROWNUM = 1)) DOLG,
						 (SELECT 'Y'
								FROM KMDW.MI_RB_ACCT_MTH
							 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE --(SELECT actdat FROM OBA_DATE)
								 AND CLIENT_NO = X.CLIENT_NO
								 AND ACCT_TYPE = 'MDP'
								 AND ACCT_STATUS != 'C'
								 AND ROWNUM = 1) PDOLG,
						 --(
						 NULL AS ING
			--SELECT 'Y'
			--FROM   REPGA_TMPM_OBA_ING
			--WHERE  sym_accno = x.account_no
			--) ing
				FROM (SELECT CLIENT_NO,
										 ACCOUNT_NO,
										 CCY,
										 SAP_AMOUNT BAL_AMT,
										 AMOUNT_HUF BAL_HUF,
										 (SELECT SAP_AMOUNT
												FROM REPGA_TMPM_OBA_FIDB
											 WHERE SAP_FLAG = 'I'
												 AND ACCOUNT_NO = X.ACCOUNT_NO) INT_AMT,
										 (SELECT AMOUNT_HUF
												FROM REPGA_TMPM_OBA_FIDB
											 WHERE SAP_FLAG = 'I'
												 AND ACCOUNT_NO = X.ACCOUNT_NO) INT_HUF
								FROM REPGA_TMPM_OBA_FIDB X
							 WHERE SAP_FLAG = 'B'
							UNION ALL
							SELECT CLIENT_NO,
										 ACCOUNT_NO,
										 CCY,
										 0 BAL_AMT,
										 0 BAL_HUF,
										 SAP_AMOUNT INT_AMT,
										 AMOUNT_HUF INT_HUF
								FROM REPGA_TMPM_OBA_FIDB
							 WHERE ACCOUNT_NO IN
										 (SELECT ACCOUNT_NO
												FROM REPGA_TMPM_OBA_FIDB
											 WHERE SAP_FLAG = 'I'
											MINUS
											SELECT ACCOUNT_NO FROM REPGA_TMPM_OBA_FIDB WHERE SAP_FLAG = 'B')) X);
	BEGIN
		REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
		OPEN HC;
		LOOP
			--REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION || ' ' || TO_CHAR(L_ROWNUMBER));
			L_CLIENT_NO.DELETE;

			FETCH HC BULK COLLECT
				INTO L_CLIENT_NO, L_ACCOUNT_NO, L_CCY, L_BAL_AMT, L_BAL_HUF, L_INT_AMT, L_INT_HUF, L_TAX_FLAG, L_TIPUS, L_IK, L_UL, L_DOLG, L_PDOLG, L_ING --,
			--L_TAX_RATE,
			--L_INT_TAX,
			--L_RESTRAINT,
			--L_RES_INT,
			--L_RES_TAX,
			--L_RELX
			LIMIT G_BATCHSIZE;
			L_LAST := L_CLIENT_NO.COUNT;
			EXIT WHEN L_LAST = 0;
			L_ROWNUMBER := L_ROWNUMBER + L_LAST;

			FORALL X IN 1 .. L_LAST
				INSERT INTO REPGA_DETM_OBA_FIDB_R
					(SYM_RUN_DATE,
					 CLIENT_NO,
					 ACCOUNT_NO,
					 CCY,
					 BAL_AMT,
					 BAL_HUF,
					 INT_AMT,
					 INT_HUF,
					 TAX_FLAG,
					 TIPUS,
					 IK,
					 UL,
					 DOLG,
					 PDOLG,
					 ING,
					 --TAX_RATE,
					 --INT_TAX,
					 --RESTRAINT,
					 --RES_INT,
					 --RES_TAX,
					 --RELX,
					 RUN_ID)
				VALUES
					(P_SYM_RUN_DATE,
					 L_CLIENT_NO(X),
					 L_ACCOUNT_NO(X),
					 L_CCY(X),
					 L_BAL_AMT(X),
					 L_BAL_HUF(X),
					 L_INT_AMT(X),
					 L_INT_HUF(X),
					 L_TAX_FLAG(X),
					 L_TIPUS(X),
					 L_IK(X),
					 L_UL(X),
					 L_DOLG(X),
					 L_PDOLG(X),
					 L_ING(X),
					 --L_TAX_RATE(X),
					 --L_INT_TAX(X),
					 --L_RESTRAINT(X),
					 --L_RES_INT(X),
					 --L_RES_TAX(X),
					 --L_RELX(X),
					 G_RUNID);
			IF L_INDEX >= G_COMMITPOINT THEN
				COMMIT;
				L_INDEX := 0;
			ELSE
				L_INDEX := L_INDEX + L_LAST;
			END IF;
		END LOOP;
		COMMIT;
		CLOSE HC;
		G_PROCESSED := G_PROCESSED + L_ROWNUMBER;

		-- Az ügyvédi stb. elkülönített számlákon lévõ összegek a betétbiztosítás szempontjából betétnek minõsülnek,
		-- ezért azok külön kezelése indokolatlan, ezt az adatszolgáltatásban ne tüntessék fel.
		DELETE FROM REPGA_DETM_OBA_FIDB_R WHERE UL = 'Y';

		DELETE FROM REPGA_DETM_OBA_FIDB_R
		 WHERE TIPUS = 'B'
			 AND NVL(BAL_AMT,
							 0) = 0
			 AND NVL(INT_AMT,
							 0) = 0;

		COMMIT;

		UPDATE REPGA_DETM_OBA_FIDB_R X
			 SET TAX_RATE = (SELECT (SELECT TAX_RATE
																 FROM REPGA_TMPM_OBA_TAX_RATE
																WHERE COUNTRY = OT.COUNTRY_LOC)
												 FROM REPGA_TMPM_OBA_TAX OT
												WHERE CLIENT_NO = X.CLIENT_NO)
		 WHERE TAX_FLAG = 'Y';

		COMMIT;

		UPDATE REPGA_DETM_OBA_FIDB_R
			 SET INT_TAX = ROUND(INT_HUF * TAX_RATE,
													 0)
		 WHERE TAX_FLAG = 'Y';

		COMMIT;

		BEGIN
			/*FOR REC IN (SELECT IK,
												 CCY
										FROM REPGA_DETM_OBA_FIDB_R
									 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE)
			LOOP*/
				--
				UPDATE REPGA_DETM_OBA_FIDB_R REC
					 SET RESTRAINT = (
														--SELECT SUM(
														--    (
														--    SELECT ROUND(r.pledged_amt*central_bank_rate,0)
														--    FROM   OBA_ARF
														--    WHERE  ccy = rec.ccy
														--    )) restraint
														SELECT SUM(ROUND(KMDW.CONV_AMT_MTH(P_SYM_RUN_DATE,
																																REC.CCY,
																																R.PLEDGED_AMT,
																																'HUF'),
																							0)) RESTRAINT
															FROM KMDW.MI_RB_RESTRAINTS_MTH R
														 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE --(SELECT actdat FROM OBA_DATE)
															 AND SYM_RUN_DATE BETWEEN START_DATE AND END_DATE
															 AND RESTRAINT_TYPE IN
																	 (SELECT DATA_1
																			FROM REPGA_INT_MAPPING_DATA T
																		 WHERE MAPPING_ID =
																					 (SELECT MAPPING_ID
																							FROM REPGA_INT_MAPPINGS
																						 WHERE MAPPING_SHORT_NAME = 'OBA_REST')
																			 AND RETURN_VALUE = 'Y'
																			 AND SYSTIMESTAMP BETWEEN FROM_DATE AND TO_DATE) --('ZGD','AMF','DP1','DP~','FTH','CLR','AMS','AMO','KAR','KAA','RMP','DDN','CDZ','ZSK','EME','EM2')
                                       --DL: 'AMS' ÖSSZEG ZÁROLVA - EGYÉB-nél NARRATIVE mezõben 'ÜGYFÉL KÉRÉSÉRE%' esetén nem szabadna figyelembe venni a zárolást!
                                       --fenti esetben van rá konkrét példa hogy a zárolt összeg az eredeti, és nem az ügyfél által idõközben többször módosított összeget mutatja!
															 AND INTERNAL_KEY = REC.IK)
				 WHERE --IK = REC.IK AND
         SYM_RUN_DATE = P_SYM_RUN_DATE;
				--
				/*V_COMMIT := V_COMMIT + 1;
				IF V_COMMIT = 5000 THEN
					V_COMMIT := 0;
					COMMIT;
				END IF;
				--
			END LOOP;*/
			COMMIT;
		END;

		-- new version based on RESTRAINT

		--
		-- Az OBA úgy kéri ennek megadását, hogy a zárolt összeg oszlop már tartalmazza a ki nem fizethetõ int_hufot is (ha van ilyen)
		-- ebben az esetben természetesen a int_hufadó se tartalmazza a zárolt összeg int_hufára jutó int_hufadót
		--

		UPDATE REPGA_DETM_OBA_FIDB_R
			 SET RES_INT = ROUND(INT_HUF * RESTRAINT / BAL_HUF,
													 0)
		 WHERE RESTRAINT > 0
			 AND INT_HUF > 0
			 AND BAL_HUF > 0
			 AND SYM_RUN_DATE = P_SYM_RUN_DATE;

		UPDATE REPGA_DETM_OBA_FIDB_R
			 SET RES_TAX = ROUND(INT_TAX * RES_INT / INT_HUF,
													 0)
		 WHERE RES_INT > 0
			 AND INT_TAX > 0
			 AND INT_HUF > 0
			 AND SYM_RUN_DATE = P_SYM_RUN_DATE;

		COMMIT;

		UPDATE REPGA_DETM_OBA_FIDB_R
			 SET INT_HUF = INT_HUF - RES_INT
		 WHERE RES_INT > 0
			 AND SYM_RUN_DATE = P_SYM_RUN_DATE;

		UPDATE REPGA_DETM_OBA_FIDB_R
			 SET INT_TAX = INT_TAX - RES_TAX
		 WHERE RES_TAX > 0
			 AND SYM_RUN_DATE = P_SYM_RUN_DATE;

		UPDATE REPGA_DETM_OBA_FIDB_R
			 SET RESTRAINT = RESTRAINT - NVL(RES_INT,
																			 0) + NVL(RES_TAX,
																								0)
		 WHERE RESTRAINT > 0
			 AND SYM_RUN_DATE = P_SYM_RUN_DATE;

		COMMIT;

		INSERT INTO REPGA_TMPM_OBA_RELM
			(SELECT INTERNAL_KEY,
							100 / (COUNT(1) + 1) PERCENTAGE -- percentage_is_null records / clients
				 FROM MI_RB_JOINT_ACCT_MTH
				WHERE RELATION_TYPE IN ('JS', 'JJ', 'BU')
					AND SYM_RUN_DATE = P_SYM_RUN_DATE --(SELECT actdat FROM OBA_DATE)
					AND INTERNAL_KEY IN
							(SELECT IK FROM REPGA_DETM_OBA_FIDB_R WHERE SYM_RUN_DATE = P_SYM_RUN_DATE)
				GROUP BY INTERNAL_KEY);
		COMMIT;

		INSERT INTO REPGA_TMPM_OBA_REL
			(SELECT INTERNAL_KEY,
							CLIENT_NO,
							(SELECT PERCENTAGE FROM REPGA_TMPM_OBA_RELM WHERE INTERNAL_KEY = RJA.INTERNAL_KEY) PERCENTAGE
				 FROM MI_RB_JOINT_ACCT_MTH RJA
				WHERE RELATION_TYPE IN ('JS', 'JJ', 'BU')
					AND SYM_RUN_DATE = P_SYM_RUN_DATE --(SELECT actdat FROM OBA_DATE)
					AND INTERNAL_KEY IN (SELECT INTERNAL_KEY FROM REPGA_TMPM_OBA_RELM));
		COMMIT;

		INSERT INTO REPGA_TMPM_OBA_REL
			SELECT S.INTERNAL_KEY,
						 (SELECT CLIENT_NO
								FROM REPGA_DETM_OBA_FIDB_R
							 WHERE IK = S.INTERNAL_KEY
								 AND SYM_RUN_DATE = P_SYM_RUN_DATE) CLIENT_NO,
						 100 - S.PERCENTAGE REMAIN_PERCENTAGE
				FROM (SELECT INTERNAL_KEY,
										 SUM(PERCENTAGE) PERCENTAGE
								FROM REPGA_TMPM_OBA_REL
							 GROUP BY INTERNAL_KEY
							HAVING SUM(PERCENTAGE) < 99) S;
		COMMIT;

		UPDATE REPGA_DETM_OBA_FIDB_R
			 SET RELX = 'X'
		 WHERE IK IN (SELECT INTERNAL_KEY
										FROM REPGA_TMPM_OBA_REL
									 GROUP BY INTERNAL_KEY
									HAVING SUM(PERCENTAGE) >= 99)
			 AND SYM_RUN_DATE = P_SYM_RUN_DATE;
		COMMIT;

		DECLARE
			V_COMMIT NUMBER := 0;
		BEGIN
			FOR REC IN (SELECT IK,
												 CLIENT_NO,
												 ACCOUNT_NO,
												 ING,
												 TAX_FLAG,
												 INT_TAX,
												 TIPUS,
												 UL,
												 CCY,
												 COALESCE((SELECT DECODE(REPORTING,
																								'EM',
																								'Y',
																								NULL)
																		FROM KMDW.MI_FM_CLIENT_MTH
																	 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE --(SELECT actdat FROM OBA_DATE)
																		 AND CLIENT_NO = X.CLIENT_NO
																		 AND ROWNUM = 1),
																	(SELECT 'Y'
																		 FROM KMDW.MI_RB_ACCT_MTH
																		WHERE SYM_RUN_DATE = P_SYM_RUN_DATE --(SELECT actdat FROM OBA_DATE)
																			AND CLIENT_NO = X.CLIENT_NO
																			AND ACCT_TYPE IN ('DFN', 'DF7', 'DFP', 'DFM', 'MF2')
																			AND ACCT_STATUS != 'C'
																			AND ROWNUM = 1)) DOLG,
												 (SELECT 'Y'
														FROM KMDW.MI_RB_ACCT_MTH
													 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE --(SELECT actdat FROM OBA_DATE)
														 AND CLIENT_NO = X.CLIENT_NO
														 AND ACCT_TYPE = 'MDP'
														 AND ACCT_STATUS != 'C'
														 AND ROWNUM = 1) PDOLG,
												 ROUND(PERCENTAGE * BAL_AMT / 100,
															 0) BAL_AMT,
												 ROUND(PERCENTAGE * BAL_HUF / 100,
															 0) BAL_HUF,
												 ROUND(PERCENTAGE * INT_AMT / 100,
															 0) INT_AMT,
												 ROUND(PERCENTAGE * INT_HUF / 100,
															 0) INT_HUF,
												 ROUND(PERCENTAGE * RESTRAINT / 100,
															 0) RESTRAINT
										FROM (SELECT IK,
																 X.CLIENT_NO,
																 PERCENTAGE,
																 R.TIPUS,
																 ING,
																 TAX_FLAG,
																 INT_TAX,
																 R.ACCOUNT_NO,
																 R.UL,
																 R.CCY,
																 R.BAL_AMT,
																 R.BAL_HUF,
																 R.INT_AMT,
																 R.INT_HUF,
																 R.RESTRAINT
														FROM REPGA_TMPM_OBA_REL    X,
																 REPGA_DETM_OBA_FIDB_R R
													 WHERE R.IK = X.INTERNAL_KEY
														 AND R.SYM_RUN_DATE = P_SYM_RUN_DATE
														 AND RELX = 'X') X)
			LOOP
				--
				INSERT INTO REPGA_DETM_OBA_FIDB_R
					(SYM_RUN_DATE,
					 ACCOUNT_NO,
					 CLIENT_NO,
					 TIPUS,
					 IK,
					 UL,
					 CCY,
					 DOLG,
					 PDOLG,
					 BAL_AMT,
					 BAL_HUF,
					 INT_AMT,
					 INT_HUF,
					 RESTRAINT,
					 ING,
					 TAX_FLAG,
					 INT_TAX,
					 RUN_ID)
				VALUES
					(P_SYM_RUN_DATE,
					 REC.ACCOUNT_NO,
					 REC.CLIENT_NO,
					 REC.TIPUS,
					 REC.IK,
					 REC.UL,
					 REC.CCY,
					 REC.DOLG,
					 REC.PDOLG,
					 REC.BAL_AMT,
					 REC.BAL_HUF,
					 REC.INT_AMT,
					 REC.INT_HUF,
					 REC.RESTRAINT,
					 REC.ING,
					 REC.TAX_FLAG,
					 REC.INT_TAX,
					 G_RUNID);
				--
				V_COMMIT := V_COMMIT + 1;
				IF V_COMMIT = 1000 THEN
					V_COMMIT := 0;
					COMMIT;
				END IF;
				--
			END LOOP;
			COMMIT;
		END;

		DELETE FROM REPGA_DETM_OBA_FIDB_R
		 WHERE RELX = 'X'
			 AND SYM_RUN_DATE = P_SYM_RUN_DATE;
		COMMIT;

	END;
	----------------------------------------
	PROCEDURE PREPROCESS(P_SYM_RUN_DATE DATE) IS
		C_ACCTION CONSTANT VARCHAR2(10) := 'PREPROCESS';
	BEGIN
		REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
		G_BATCHSIZE   := REPGA_UTIL_PKG.GET_BATCHSIZE(GC_TARGETTABLE,
																									G_BATCHSIZE);
		G_COMMITPOINT := REPGA_UTIL_PKG.GET_COMMITPOINT(GC_TARGETTABLE,
																										G_COMMITPOINT);
		REPGA_UTIL_PKG.CLEANUP(P_SYM_RUN_DATE => P_SYM_RUN_DATE,
													 P_APPNAME      => GC_APPNAME,
													 P_TARGETNAME   => GC_TARGETTABLE);
		EXECUTE IMMEDIATE 'TRUNCATE TABLE REPGA_TMPM_OBA_TI';
		EXECUTE IMMEDIATE 'TRUNCATE TABLE REPGA_TMPM_OBA_TAX_RATE';
		EXECUTE IMMEDIATE 'TRUNCATE TABLE REPGA_TMPM_OBA_TAX';
		EXECUTE IMMEDIATE 'TRUNCATE TABLE REPGA_TMPM_OBA_AGA';
		EXECUTE IMMEDIATE 'TRUNCATE TABLE REPGA_TMPM_OBA_FIDB';
		EXECUTE IMMEDIATE 'TRUNCATE TABLE REPGA_TMPM_OBA_RELM';
		EXECUTE IMMEDIATE 'TRUNCATE TABLE REPGA_TMPM_OBA_REL';
		INSERT_TMPM_OBA_TI(P_SYM_RUN_DATE);
		INSERT_TMPM_OBA_TAX_RATE(P_SYM_RUN_DATE);
		INSERT_TMPM_OBA_TAX(P_SYM_RUN_DATE);
		INSERT_TMPM_OBA_AGA(P_SYM_RUN_DATE);
		INSERT_TMPM_OBA_FIDB(P_SYM_RUN_DATE);
	END;

	----------------------------------------
	PROCEDURE POSTPROCESS(P_SYM_RUN_DATE DATE) IS
		C_ACCTION CONSTANT VARCHAR2(11) := 'POSTPROCESS';
	BEGIN
		REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
		REPGA_UTIL_PKG.HOUSE_KEEPING(P_SYM_RUN_DATE => P_SYM_RUN_DATE,
																 P_APPNAME      => GC_APPNAME,
																 P_TARGETNAME   => GC_TARGETTABLE);
	END;
	----------------------------------------
	PROCEDURE MAIN(P_SYM_RUN_DATE DATE DEFAULT EBH_PWM_PUBLIC_P.F_GETSYMRUNDATE) IS
	BEGIN
		G_RUNID     := REPGA_BACKOFFICE_PKG.START_API(P_APPNAME      => GC_APPNAME,
																									P_SYM_RUN_DATE => P_SYM_RUN_DATE);
		G_PROCESSED := 0;
		PREPROCESS(P_SYM_RUN_DATE);
		DELETE_TABLE(P_SYM_RUN_DATE);
		INSERT_TABLE(P_SYM_RUN_DATE);
		POSTPROCESS(P_SYM_RUN_DATE);
		REPGA_BACKOFFICE_PKG.END_API(G_PROCESSED);
	EXCEPTION
		WHEN OTHERS THEN
			REPGA_BACKOFFICE_PKG.END_ERROR_API(SQLCODE,
																				 SQLERRM);
	END;

END;
/
