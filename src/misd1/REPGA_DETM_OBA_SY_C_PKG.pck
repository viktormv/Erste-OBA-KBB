CREATE OR REPLACE PACKAGE REPGA_DETM_OBA_SY_C_PKG IS
	-- %version   0.1   2012.10.25.
	-- %author   Dörnyei László
	-- %usage loading package for detail area
	-- %intab tablelist:  REPGA_DETM_OBA_FIDB_R<BR/>
	--                    KMDW.MI_FM_CLIENT_MTHH<BR/>
	--                    KMDW.MI_FM_CLIENT_INDVL_MTHH<BR/>
	--                    KMDW.MI_FM_CLIENT_CONTACT_MTH<BR/>
	--                    REPGA_DETM_OBA_MATS<BR/>

	-- %outtab tablelist: REPGA_DETM_OBA_SY_C

	PROCEDURE MAIN(P_SYM_RUN_DATE DATE DEFAULT EBH_PWM_PUBLIC_P.F_GETSYMRUNDATE);
	FUNCTION P_ADDR_CORRECT(P_NAME IN VARCHAR2) RETURN VARCHAR2;
END;
/
CREATE OR REPLACE PACKAGE BODY REPGA_DETM_OBA_SY_C_PKG IS
	-- %version   0.1   2012.10.25.
	-- %author   Dörnyei László
	--
	-- Procedure for calculating and loading data into REPGA_DETM_OBA_SY_C table
	-- %param p_sym_run_data: symbols running time <br>
	--              in form of date type
	-- %raises NO
	-- %dynasql YES
	-- %autonom NO
	-- %dbmsout NO
	----------------------------------------------------------------------------------
	-- %version   0.1   DL ERSTEOBA-3 INITIAL

	GC_APPNAME CONSTANT VARCHAR2(30) := 'REPGA_DETM_OBA_SY_C_PKG';

	GC_TARGETTABLE CONSTANT VARCHAR2(30) := 'REPGA_DETM_OBA_SY_C';

	G_PROCESSED NUMBER := 0;

	G_RUNID NUMBER;

	G_COMMITPOINT NUMBER := 200000;

	G_BATCHSIZE NUMBER := 25000;
	----------------------------------------------------------------------------------
	FUNCTION P_ADDR_CORRECT(P_NAME IN VARCHAR2) RETURN VARCHAR2 IS
		V_NAME VARCHAR2(300);
	BEGIN
		V_NAME := P_NAME;
		--
		BEGIN
			V_NAME := REPLACE( -- removing characters ordinary not used in names
												REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM( -- removing unnecessary spaces
																																																																																																																																																																										 V_NAME),
																																																																																																																																																																								'§',
																																																																																																																																																																								' '),
																																																																																																																																																																				' ''',
																																																																																																																																																																				' '),
																																																																																																																																																																'~',
																																																																																																																																																																' '),
																																																																																																																																																												'"',
																																																																																																																																																												' '),
																																																																																																																																																								'+',
																																																																																																																																																								' '),
																																																																																																																																																				'^',
																																																																																																																																																				' '),
																																																																																																																																																'!',
																																																																																																																																																' '),
																																																																																																																																												'%',
																																																																																																																																												' '),
																																																																																																																																								'=',
																																																																																																																																								' '),
																																																																																																																																				'(',
																																																																																																																																				' '),
																																																																																																																																')',
																																																																																																																																' '),
																																																																																																																												'÷',
																																																																																																																												' '),
																																																																																																																								'×',
																																																																																																																								' '),
																																																																																																																				'[',
																																																																																																																				' '),
																																																																																																																']',
																																																																																																																' '),
																																																																																																												'$',
																																																																																																												' '),
																																																																																																								'¤',
																																																																																																								' '),
																																																																																																				'<',
																																																																																																				' '),
																																																																																																'>',
																																																																																																' '),
																																																																																												'#',
																																																																																												' '),
																																																																																								'&',
																																																																																								' '),
																																																																																				'@',
																																																																																				' '),
																																																																																'{',
																																																																																' '),
																																																																												'}',
																																																																												' '),
																																																																								',',
																																																																								' '),
																																																																				'?',
																																																																				' '),
																																																																';',
																																																																' '),
																																																												':',
																																																												' '),
																																																								'*',
																																																								' '),
																																																				'_',
																																																				' '),
																																																'           ',
																																																' '),
																																												'          ',
																																												' '),
																																								'         ',
																																								' '),
																																				'        ',
																																				' '),
																																'       ',
																																' '),
																												'      ',
																												' '),
																								'     ',
																								' '),
																				'    ',
																				' '),
																'   ',
																' '),
												'  ',
												' ');
			--
		END;
		RETURN V_NAME;
	END P_ADDR_CORRECT;

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
	
		--TYPE T_SYM_RUN_DATE IS TABLE OF REPGA_DETM_OBA_SY_C.SYM_RUN_DATE%TYPE INDEX BY PLS_INTEGER;
		TYPE T_CLIENT_NO IS TABLE OF REPGA_DETM_OBA_SY_C.CLIENT_NO%TYPE INDEX BY PLS_INTEGER;
		TYPE T_CLIENT_NAME IS TABLE OF REPGA_DETM_OBA_SY_C.CLIENT_NAME%TYPE INDEX BY PLS_INTEGER;
		TYPE T_MAIDEN_NAME IS TABLE OF REPGA_DETM_OBA_SY_C.MAIDEN_NAME%TYPE INDEX BY PLS_INTEGER;
		TYPE T_MOTHER_MAIDEN_NAME IS TABLE OF REPGA_DETM_OBA_SY_C.MOTHER_MAIDEN_NAME%TYPE INDEX BY PLS_INTEGER;
		TYPE T_BIRTH_DATE IS TABLE OF REPGA_DETM_OBA_SY_C.BIRTH_DATE%TYPE INDEX BY PLS_INTEGER;
		TYPE T_BIRTH_PLACE IS TABLE OF REPGA_DETM_OBA_SY_C.BIRTH_PLACE%TYPE INDEX BY PLS_INTEGER;
		TYPE T_COUNTRY_CITIZEN IS TABLE OF REPGA_DETM_OBA_SY_C.COUNTRY_CITIZEN%TYPE INDEX BY PLS_INTEGER;
		TYPE T_SZIGSZ IS TABLE OF REPGA_DETM_OBA_SY_C.SZIGSZ%TYPE INDEX BY PLS_INTEGER;
		TYPE T_UTLEV IS TABLE OF REPGA_DETM_OBA_SY_C.UTLEV%TYPE INDEX BY PLS_INTEGER;
		TYPE T_JOGSI IS TABLE OF REPGA_DETM_OBA_SY_C.JOGSI%TYPE INDEX BY PLS_INTEGER;
		TYPE T_CEGJSZAM IS TABLE OF REPGA_DETM_OBA_SY_C.CEGJSZAM%TYPE INDEX BY PLS_INTEGER;
		TYPE T_KSH IS TABLE OF REPGA_DETM_OBA_SY_C.KSH%TYPE INDEX BY PLS_INTEGER;
		TYPE T_ADOAZ IS TABLE OF REPGA_DETM_OBA_SY_C.ADOAZ%TYPE INDEX BY PLS_INTEGER;
		TYPE T_ADOSZAM IS TABLE OF REPGA_DETM_OBA_SY_C.ADOSZAM%TYPE INDEX BY PLS_INTEGER;
		TYPE T_LOC_CODE IS TABLE OF REPGA_DETM_OBA_SY_C.LOC_CODE%TYPE INDEX BY PLS_INTEGER;
		TYPE T_LOC_CITY IS TABLE OF REPGA_DETM_OBA_SY_C.LOC_CITY%TYPE INDEX BY PLS_INTEGER;
		TYPE T_LOC_STREET IS TABLE OF REPGA_DETM_OBA_SY_C.LOC_STREET%TYPE INDEX BY PLS_INTEGER;
		TYPE T_UGYFTIPUS IS TABLE OF REPGA_DETM_OBA_SY_C.UGYFTIPUS%TYPE INDEX BY PLS_INTEGER;
		TYPE T_DOLG IS TABLE OF REPGA_DETM_OBA_SY_C.DOLG%TYPE INDEX BY PLS_INTEGER;
		TYPE T_PDOLG IS TABLE OF REPGA_DETM_OBA_SY_C.PDOLG%TYPE INDEX BY PLS_INTEGER;
		--TYPE T_BAL_HUF IS TABLE OF REPGA_DETM_OBA_SY_C.BAL_HUF%TYPE INDEX BY PLS_INTEGER;
		--TYPE T_INT_HUF IS TABLE OF REPGA_DETM_OBA_SY_C.INT_HUF%TYPE INDEX BY PLS_INTEGER;
		--TYPE T_INT_TAX IS TABLE OF REPGA_DETM_OBA_SY_C.INT_TAX%TYPE INDEX BY PLS_INTEGER;
		--TYPE T_REST IS TABLE OF REPGA_DETM_OBA_SY_C.REST%TYPE INDEX BY PLS_INTEGER;
		--TYPE T_MATURED IS TABLE OF REPGA_DETM_OBA_SY_C.MATURED%TYPE INDEX BY PLS_INTEGER;
		--TYPE T_PHONE IS TABLE OF REPGA_DETM_OBA_SY_C.PHONE%TYPE INDEX BY PLS_INTEGER;
		--TYPE T_EMAIL IS TABLE OF REPGA_DETM_OBA_SY_C.EMAIL%TYPE INDEX BY PLS_INTEGER;
		--TYPE T_RUN_ID IS TABLE OF REPGA_DETM_OBA_SY_C.RUN_ID%TYPE INDEX BY PLS_INTEGER;
	
		--L_SYM_RUN_DATE  T_SYM_RUN_DATE;
		L_CLIENT_NO          T_CLIENT_NO;
		L_CLIENT_NAME        T_CLIENT_NAME;
		L_MAIDEN_NAME        T_MAIDEN_NAME;
		L_MOTHER_MAIDEN_NAME T_MOTHER_MAIDEN_NAME;
		L_BIRTH_DATE         T_BIRTH_DATE;
		L_BIRTH_PLACE        T_BIRTH_PLACE;
		L_COUNTRY_CITIZEN    T_COUNTRY_CITIZEN;
		L_SZIGSZ             T_SZIGSZ;
		L_UTLEV              T_UTLEV;
		L_JOGSI              T_JOGSI;
		L_CEGJSZAM           T_CEGJSZAM;
		L_KSH                T_KSH;
		L_ADOAZ              T_ADOAZ;
		L_ADOSZAM            T_ADOSZAM;
		L_LOC_CODE           T_LOC_CODE;
		L_LOC_CITY           T_LOC_CITY;
		L_LOC_STREET         T_LOC_STREET;
		L_UGYFTIPUS          T_UGYFTIPUS;
		L_DOLG               T_DOLG;
		L_PDOLG              T_PDOLG;
		--L_BAL_HUF  T_BAL_HUF;
		--L_INT_HUF  T_INT_HUF;
		--L_INT_TAX  T_INT_TAX;
		--L_REST  T_REST;
		--L_MATURED  T_MATURED;
		--L_PHONE  T_PHONE;
		--L_EMAIL  T_EMAIL;
		--L_RUN_ID  T_RUN_ID;
	
		CURSOR HC IS(
			SELECT A.CLIENT_NO,
						 A.CLIENT_NAME,
						 MAIDEN_SURNAME || ' ' || MAIDEN_FIRST_NAME MAIDEN_NAME,
						 MOTHER_MAIDEN_NAME,
						 BIRTH_DATE,
						 BIRTH_PLACE,
						 COUNTRY_CITIZEN,
						 DECODE(GLOBAL_ID_TYPE,
										'NIC',
										GLOBAL_ID,
										DECODE(GLOBAL_ID_TYPE2,
													 'NIC',
													 GLOBAL_ID2,
													 NULL)) SZIGSZ,
						 DECODE(GLOBAL_ID_TYPE,
										'PPT',
										GLOBAL_ID,
										DECODE(GLOBAL_ID_TYPE2,
													 'PPT',
													 GLOBAL_ID2,
													 NULL)) UTLEV,
						 DECODE(GLOBAL_ID_TYPE,
										'KVE',
										GLOBAL_ID,
										DECODE(GLOBAL_ID_TYPE2,
													 'KVE',
													 GLOBAL_ID2,
													 NULL)) JOGSI,
						 DECODE(GLOBAL_ID_TYPE,
										'BRN',
										GLOBAL_ID,
										DECODE(GLOBAL_ID_TYPE2,
													 'BRN',
													 GLOBAL_ID2,
													 NULL)) CEGJSZAM,
						 TAX_FILE_NO KSH,
						 DECODE(LENGTH(TAX_NO),
										10,
										TAX_NO) ADOAZ,
						 DECODE(LENGTH(TAX_NO),
										11,
										TAX_NO) ADOSZAM,
						 TRIM(SUBSTR(LOCATION,
												 1,
												 5)) LOC_CODE,
						 TRIM(SUBSTR(LOCATION,
												 6,
												 20)) LOC_CITY,
						 P_ADDR_CORRECT(SUBSTR(LOCATION,
																	 26,
																	 25)) LOC_STREET,
						 --SUBSTR(location,26,25) loc_street,
						 CASE
							 WHEN (UPPER(CLIENT_NAME) || ' ' LIKE '% WX %' OR
										' ' || UPPER(CLIENT_NAME) || ' ' LIKE '% ELHUNYT %' OR
										' ' || UPPER(CLIENT_NAME) || ' ' LIKE '% NÉHAI %' OR CLIENT_STATUS = 'MHT') THEN
								'E'
							 WHEN A.MAJOR_CATEGORY IN ('PR', 'EM') AND A.PROFIT_SEGMENT IN ('0100', '0120') THEN
								'M'
							 WHEN A.MAJOR_CATEGORY != 'PR' AND A.MAJOR_CATEGORY != 'EM' AND
										A.PROFIT_SEGMENT IN ('0100', '0120') THEN
								'C'
							 WHEN A.PROFIT_SEGMENT IN ('0311', '0313', '0317', '0341', '0342', '0344', '0345',
										 '0371', '0394', '0395', '0396', '0397', '0398') THEN
								'C'
							 WHEN A.PROFIT_SEGMENT IN ('0399', '0350') THEN
								'T'
							 WHEN A.PROFIT_SEGMENT LIKE '03%' THEN
								'C'
							 ELSE
								'M'
						 END UGYFTIPUS,
						 (SELECT 'Y'
								FROM REPGA_DETM_OBA_FIDB_R
							 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
								 AND CLIENT_NO = A.CLIENT_NO
								 AND DOLG = 'Y'
								 AND ROWNUM = 1) DOLG,
						 (SELECT 'Y'
								FROM REPGA_DETM_OBA_FIDB_R
							 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
								 AND CLIENT_NO = A.CLIENT_NO
								 AND PDOLG = 'Y'
								 AND ROWNUM = 1) PDOLG
				FROM KMDW.MI_FM_CLIENT_MTH A,
						 (SELECT *
								FROM KMDW.MI_FM_CLIENT_INDVL_MTH
							 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE --(SELECT actdat FROM OBA_DATE)
							) I
			 WHERE EXISTS (SELECT 1
								FROM REPGA_DETM_OBA_FIDB_R
							 WHERE CLIENT_NO = A.CLIENT_NO
								 AND SYM_RUN_DATE = P_SYM_RUN_DATE)
				 AND A.CLIENT_NO = I.CLIENT_NO(+)
				 AND A.SYM_RUN_DATE = P_SYM_RUN_DATE --(SELECT actdat FROM OBA_DATE)
			 );
	BEGIN
		REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
		OPEN HC;
		LOOP
			--REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION || ' ' || TO_CHAR(L_ROWNUMBER));
			L_CLIENT_NO.DELETE;
		
			FETCH HC BULK COLLECT
				INTO
			--L_SYM_RUN_DATE,
			L_CLIENT_NO, L_CLIENT_NAME, L_MAIDEN_NAME, L_MOTHER_MAIDEN_NAME, L_BIRTH_DATE, L_BIRTH_PLACE, L_COUNTRY_CITIZEN, L_SZIGSZ, L_UTLEV, L_JOGSI, L_CEGJSZAM, L_KSH, L_ADOAZ, L_ADOSZAM, L_LOC_CODE, L_LOC_CITY, L_LOC_STREET, L_UGYFTIPUS, L_DOLG, L_PDOLG
			--L_BAL_HUF,
			--L_INT_HUF,
			--L_INT_TAX,
			--L_REST,
			--L_MATURED,
			--L_PHONE,
			--L_EMAIL,
			--L_RUN_ID
			LIMIT G_BATCHSIZE;
			L_LAST := L_CLIENT_NO.COUNT;
			EXIT WHEN L_LAST = 0;
			L_ROWNUMBER := L_ROWNUMBER + L_LAST;
		
			FORALL X IN 1 .. L_LAST
				INSERT INTO REPGA_DETM_OBA_SY_C
					(SYM_RUN_DATE,
					 CLIENT_NO,
					 CLIENT_NAME,
					 MAIDEN_NAME,
					 MOTHER_MAIDEN_NAME,
					 BIRTH_DATE,
					 BIRTH_PLACE,
					 COUNTRY_CITIZEN,
					 SZIGSZ,
					 UTLEV,
					 JOGSI,
					 CEGJSZAM,
					 KSH,
					 ADOAZ,
					 ADOSZAM,
					 LOC_CODE,
					 LOC_CITY,
					 LOC_STREET,
					 UGYFTIPUS,
					 DOLG,
					 PDOLG,
					 BAL_HUF,
					 INT_HUF,
					 INT_TAX,
					 REST,
					 MATURED,
					 PHONE,
					 EMAIL,
					 RUN_ID)
				VALUES
					(P_SYM_RUN_DATE,
					 L_CLIENT_NO(X),
					 L_CLIENT_NAME(X),
					 L_MAIDEN_NAME(X),
					 L_MOTHER_MAIDEN_NAME(X),
					 L_BIRTH_DATE(X),
					 L_BIRTH_PLACE(X),
					 L_COUNTRY_CITIZEN(X),
					 L_SZIGSZ(X),
					 L_UTLEV(X),
					 L_JOGSI(X),
					 L_CEGJSZAM(X),
					 L_KSH(X),
					 L_ADOAZ(X),
					 L_ADOSZAM(X),
					 L_LOC_CODE(X),
					 L_LOC_CITY(X),
					 L_LOC_STREET(X),
					 L_UGYFTIPUS(X),
					 L_DOLG(X),
					 L_PDOLG(X),
					 NULL, --L_BAL_HUF(X),
					 NULL, --L_INT_HUF(X),
					 NULL, --L_INT_TAX(X),
					 NULL, --L_REST(X),
					 NULL, --L_MATURED(X),
					 NULL, --L_PHONE(X),
					 NULL, --L_EMAIL(X),
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
	
		INSERT INTO REPGA_TMPM_OBA_CONS
			(SELECT CLIENT_NO,
							-- torzsszam 
							DECODE(SUBSTR(KSH,
														1,
														8),
										 '00000000',
										 NULL,
										 '11111111',
										 NULL,
										 '22222222',
										 NULL,
										 '33333333',
										 NULL,
										 '44444444',
										 NULL,
										 '55555555',
										 NULL,
										 '66666666',
										 NULL,
										 '77777777',
										 NULL,
										 '88888888',
										 NULL,
										 '99999999',
										 NULL,
										 'ISMERETL',
										 NULL,
										 SUBSTR(KSH,
														1,
														8)) TSZ,
							-- indvl
							--mig_bi_normale_name(DECODE(maiden_name,'ISMERETLEN',NULL,maiden_name)) mn,
							--mig_bi_normale_name(DECODE(mother_maiden_name,'ISMERETLEN',NULL,mother_maiden_name)) mmn,
							DECODE(MAIDEN_NAME,
										 'ISMERETLEN',
										 NULL,
										 MAIDEN_NAME) MN,
							DECODE(MOTHER_MAIDEN_NAME,
										 'ISMERETLEN',
										 NULL,
										 MOTHER_MAIDEN_NAME) MMN,
							BIRTH_DATE,
							DECODE(BIRTH_DATE,
										 NULL,
										 0,
										 1) CONS_BIT
				 FROM REPGA_DETM_OBA_SY_C
				WHERE NVL(BIRTH_DATE,
									SYSDATE) > TO_DATE('19020101',
																		 'yyyymmdd')
					AND SYM_RUN_DATE = P_SYM_RUN_DATE);
		COMMIT;
	
		INSERT INTO REPGA_TMPM_OBA_CONS_M
			(SELECT CLIENT_NO,
							MN,
							MMN,
							BIRTH_DATE
				 FROM REPGA_TMPM_OBA_CONS
				WHERE CONS_BIT = 1
					AND (MN, MMN, BIRTH_DATE) IN (SELECT MN,
																							 MMN,
																							 BIRTH_DATE
																					FROM REPGA_TMPM_OBA_CONS
																				 WHERE CONS_BIT = 1
																				 GROUP BY MN,
																									MMN,
																									BIRTH_DATE
																				HAVING COUNT(1) > 1));
		COMMIT;
	
		INSERT INTO REPGA_TMPM_OBA_CONS_MR
			(SELECT CLIENT_NO,
							MN,
							MMN,
							BIRTH_DATE,
							ROW_NUMBER() OVER(PARTITION BY MN, MMN, BIRTH_DATE ORDER BY CLIENT_NO DESC) R_NO
				 FROM REPGA_TMPM_OBA_CONS_M);
		COMMIT;
	
		DECLARE
		BEGIN
			FOR REC IN (SELECT CLIENT_NO,
												 MN,
												 MMN,
												 BIRTH_DATE,
												 (SELECT DOLG
														FROM REPGA_DETM_OBA_SY_C
													 WHERE CLIENT_NO = X.CLIENT_NO
														 AND SYM_RUN_DATE = P_SYM_RUN_DATE) DOLG,
												 (SELECT PDOLG
														FROM REPGA_DETM_OBA_SY_C
													 WHERE CLIENT_NO = X.CLIENT_NO
														 AND SYM_RUN_DATE = P_SYM_RUN_DATE) PDOLG
										FROM REPGA_TMPM_OBA_CONS_MR X
									 WHERE R_NO = 1)
			LOOP
				FOR IREC IN (SELECT CLIENT_NO
											 FROM REPGA_TMPM_OBA_CONS_MR
											WHERE R_NO > 1
												AND MN = REC.MN
												AND MMN = REC.MMN
												AND BIRTH_DATE = REC.BIRTH_DATE)
				LOOP
					--
					UPDATE REPGA_DETM_OBA_FIDB_R
						 SET CLIENT_NO = REC.CLIENT_NO,
								 DOLG      = REC.DOLG,
								 PDOLG     = REC.PDOLG
					 WHERE CLIENT_NO = IREC.CLIENT_NO
						 AND SYM_RUN_DATE = P_SYM_RUN_DATE;
					--
				END LOOP;
			END LOOP;
			COMMIT;
		END;
	
		UPDATE REPGA_DETM_OBA_SY_C X
			 SET BAL_HUF = (SELECT SUM(BAL_HUF)
												FROM REPGA_DETM_OBA_FIDB_R
											 WHERE CLIENT_NO = X.CLIENT_NO
												 AND SYM_RUN_DATE = P_SYM_RUN_DATE),
					 INT_HUF = (SELECT SUM(INT_HUF)
												FROM REPGA_DETM_OBA_FIDB_R
											 WHERE CLIENT_NO = X.CLIENT_NO
												 AND SYM_RUN_DATE = P_SYM_RUN_DATE),
					 INT_TAX = (SELECT SUM(INT_TAX)
												FROM REPGA_DETM_OBA_FIDB_R
											 WHERE CLIENT_NO = X.CLIENT_NO
												 AND SYM_RUN_DATE = P_SYM_RUN_DATE),
					 REST    = (SELECT SUM(RESTRAINT)
												FROM REPGA_DETM_OBA_FIDB_R
											 WHERE CLIENT_NO = X.CLIENT_NO
												 AND SYM_RUN_DATE = P_SYM_RUN_DATE)
		 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE;
		COMMIT;
	
		--EXECUTE IMMEDIATE 'CREATE UNIQUE INDEX uidx_OBA_MATS ON REPGA_DETM_OBA_MATS(client_no)';
		UPDATE REPGA_DETM_OBA_SY_C X
			 SET MATURED = NVL(MATURED,
												 0) + (SELECT AMT
																 FROM REPGA_DETM_OBA_MATS
																WHERE CLIENT_NO = X.CLIENT_NO
																	AND SYM_RUN_DATE = P_SYM_RUN_DATE)
		 WHERE CLIENT_NO IN (SELECT CLIENT_NO
													 FROM REPGA_DETM_OBA_MATS
													WHERE AMT > 0
														AND SYM_RUN_DATE = P_SYM_RUN_DATE)
			 AND SYM_RUN_DATE = P_SYM_RUN_DATE;
		COMMIT;
	
		DECLARE
		BEGIN
			FOR REC IN (SELECT CLIENT_NO,
												 MN,
												 MMN,
												 BIRTH_DATE
										FROM REPGA_TMPM_OBA_CONS_MR
									 WHERE R_NO = 1)
			LOOP
				FOR IREC IN (SELECT CLIENT_NO,
														(SELECT MATURED
															 FROM REPGA_DETM_OBA_SY_C
															WHERE CLIENT_NO = X.CLIENT_NO
																AND SYM_RUN_DATE = P_SYM_RUN_DATE) MATURED
											 FROM REPGA_TMPM_OBA_CONS_MR X
											WHERE R_NO > 1
												AND MN = REC.MN
												AND MMN = REC.MMN
												AND BIRTH_DATE = REC.BIRTH_DATE)
				LOOP
					--
					UPDATE REPGA_DETM_OBA_SY_C
						 SET MATURED = NVL(MATURED,
															 0) + NVL(IREC.MATURED,
																				0)
					 WHERE CLIENT_NO = REC.CLIENT_NO
						 AND SYM_RUN_DATE = P_SYM_RUN_DATE;
					--
					DELETE FROM REPGA_DETM_OBA_SY_C
					 WHERE CLIENT_NO = IREC.CLIENT_NO
						 AND SYM_RUN_DATE = P_SYM_RUN_DATE;
				END LOOP;
			END LOOP;
			COMMIT;
		END;
	
		INSERT INTO REPGA_TMPM_OBA_SY_CC
			(SELECT CLIENT_NO,
							(SELECT REPLACE(ADDRESS,
															';',
															'')
								 FROM MI_FM_CLIENT_CONTACT_MTH
								WHERE CLIENT_NO = X.CLIENT_NO
									AND CONTACT_TYPE = 'TEL'
									AND SYM_RUN_DATE = DATE '2010-01-31') PHONE_TEL,
							(SELECT REPLACE(ADDRESS,
															';',
															'')
								 FROM MI_FM_CLIENT_CONTACT_MTH
								WHERE CLIENT_NO = X.CLIENT_NO
									AND CONTACT_TYPE = 'MOB'
									AND SYM_RUN_DATE = DATE '2010-01-31') PHONE_MOB,
							(SELECT REPLACE(CONTACT_ID,
															';',
															'')
								 FROM MI_FM_CLIENT_CONTACT_MTH
								WHERE CLIENT_NO = X.CLIENT_NO
									AND CONTACT_TYPE = 'EMA'
									AND SYM_RUN_DATE = DATE '2010-01-31') E_MAIL
				 FROM REPGA_DETM_OBA_SY_C X
				WHERE SYM_RUN_DATE = P_SYM_RUN_DATE);
		COMMIT;
	
		--EXECUTE IMMEDIATE 'CREATE UNIQUE INDEX uidx_OBA_SY_CC ON OBA_SY_CC(client_no)';
		UPDATE REPGA_DETM_OBA_SY_C X
			 SET (PHONE, EMAIL) = (SELECT PHONE_TEL || NVL2(PHONE_MOB,
																											NVL2(PHONE_TEL,
																													 ',',
																													 NULL),
																											NULL) || PHONE_MOB,
																		LOWER(E_MAIL)
															 FROM REPGA_TMPM_OBA_SY_CC
															WHERE CLIENT_NO = X.CLIENT_NO
																AND SYM_RUN_DATE = P_SYM_RUN_DATE);
	
		UPDATE REPGA_DETM_OBA_SY_C
			 SET MATURED = ROUND(MATURED)
		 WHERE MATURED IS NOT NULL
			 AND SYM_RUN_DATE = P_SYM_RUN_DATE;
		COMMIT;
	
		UPDATE REPGA_DETM_OBA_SY_C
			 SET CLIENT_NAME        = REPLACE(CLIENT_NAME,
																				'"',
																				''),
					 MAIDEN_NAME        = REPLACE(MAIDEN_NAME,
																				'"',
																				''),
					 MOTHER_MAIDEN_NAME = REPLACE(MOTHER_MAIDEN_NAME,
																				'"',
																				''),
					 BIRTH_PLACE        = REPLACE(BIRTH_PLACE,
																				'"',
																				''),
					 COUNTRY_CITIZEN    = REPLACE(COUNTRY_CITIZEN,
																				'"',
																				''),
					 SZIGSZ             = REPLACE(SZIGSZ,
																				'"',
																				''),
					 UTLEV              = REPLACE(UTLEV,
																				'"',
																				''),
					 JOGSI              = REPLACE(JOGSI,
																				'"',
																				''),
					 CEGJSZAM           = REPLACE(CEGJSZAM,
																				'"',
																				''),
					 KSH                = REPLACE(KSH,
																				'"',
																				''),
					 ADOAZ              = REPLACE(ADOAZ,
																				'"',
																				''),
					 ADOSZAM            = REPLACE(ADOSZAM,
																				'"',
																				''),
					 PHONE              = REPLACE(PHONE,
																				'"',
																				''),
					 EMAIL              = REPLACE(EMAIL,
																				'"',
																				'')
		 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE;
		COMMIT;
	
		UPDATE REPGA_DETM_OBA_SY_C
			 SET JOGSI = NULL
		 WHERE LENGTH(JOGSI) > 10
			 AND SYM_RUN_DATE = P_SYM_RUN_DATE;
		UPDATE REPGA_DETM_OBA_U_CLIENT
			 SET VEZ_ENG = NULL
		 WHERE LENGTH(VEZ_ENG) > 10
			 AND SYM_RUN_DATE = P_SYM_RUN_DATE;
		UPDATE REPGA_DETM_OBA_BJEGY
			 SET F19 = NULL
		 WHERE LENGTH(F19) > 10
			 AND SYM_RUN_DATE = P_SYM_RUN_DATE;
		UPDATE REPGA_DETM_OBA_EJEGY
			 SET F15 = NULL
		 WHERE LENGTH(F15) > 10
			 AND SYM_RUN_DATE = P_SYM_RUN_DATE;
		UPDATE REPGA_DETM_OBA_SY_C
			 SET PHONE = LENGTH(P_ADDR_CORRECT(PHONE))
		--SET    phone = LENGTH(phone)
		 WHERE LENGTH(PHONE) > 100
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
		EXECUTE IMMEDIATE 'TRUNCATE TABLE REPGA_TMPM_OBA_CONS';
		EXECUTE IMMEDIATE 'TRUNCATE TABLE REPGA_TMPM_OBA_CONS_M';
		EXECUTE IMMEDIATE 'TRUNCATE TABLE REPGA_TMPM_OBA_CONS_MR';
		EXECUTE IMMEDIATE 'TRUNCATE TABLE REPGA_TMPM_OBA_SY_CC';
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
