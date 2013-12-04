CREATE OR REPLACE PACKAGE REPGA_DETM_OBA_TBASE_PKG IS
	-- %version   0.1   2012.10.25.
	-- %author   Dörnyei László
	-- %usage loading package for detail area
	-- %intab tablelist:
	--                    REPGA_DETM_OBA_SY_C<BR/>
	--                    OBA_TAX<BR/>
	--                    REPGA_DETM_OBA_BJEGY<BR/>
	--                    REPGA_DETM_OBA_U_CLIENT<BR/>
	--                    REPGA_DETM_OBA_EJEGY<BR/>
	--                    OBA_TBASE_LAK<BR/>
	--                    REPGA_DETM_OBA_QCONTC<BR/>

	-- %outtab tablelist: REPGA_DETM_OBA_TBASE

	PROCEDURE MAIN(P_SYM_RUN_DATE DATE DEFAULT EBH_PWM_PUBLIC_P.F_GETSYMRUNDATE);

END;
/
CREATE OR REPLACE PACKAGE BODY REPGA_DETM_OBA_TBASE_PKG IS
	-- %version   0.1   2012.10.25.
	-- %author   Dörnyei László
	--
	-- Procedure for calculating and loading data into REPGA_DETM_OBA_TBASE table
	-- %param p_sym_run_data: symbols running time <br>
	--              in form of date type
	-- %raises NO
	-- %dynasql YES
	-- %autonom NO
	-- %dbmsout NO
	----------------------------------------------------------------------------------
	-- %version   0.1   DL ERSTEOBA-3 INITIAL
    --                  G_PROCESSED töltése

	GC_APPNAME CONSTANT VARCHAR2(30) := 'REPGA_DETM_OBA_TBASE_PKG';

	GC_TARGETTABLE CONSTANT VARCHAR2(30) := 'REPGA_DETM_OBA_TBASE';

	G_PROCESSED NUMBER := 0;

	G_RUNID NUMBER;

	G_COMMITPOINT NUMBER := 200000;

	G_BATCHSIZE NUMBER := 25000;

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

	BEGIN
		REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);

		--
		-- Symbols Clients
		--
		INSERT /*+APPEND*/
		INTO REPGA_DETM_OBA_TBASE
			(SYM_RUN_DATE,
			 RUN_ID,
			 NEW_CSOP_ID,
			 AZONOSITO,
			 POSTAL_CODE,
			 CITY,
			 ADDRESS,
			 TIPUS,
			 SRC_SYS)
			SELECT P_SYM_RUN_DATE,
						 G_RUNID,
						 ROWNUM,
						 CLIENT_NO,
						 LOC_CODE,
						 LOC_CITY,
						 LOC_STREET,
						 (SELECT CLIENT_CAT FROM REPGA_TMPM_OBA_TAX WHERE CLIENT_NO = X.CLIENT_NO) TIPUS,
						 0 SRC_SYS
				FROM REPGA_DETM_OBA_SY_C X
			 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE;
		COMMIT;

		--
		-- Betéti Clients
		--
		DECLARE
			V_ID NUMBER;
		BEGIN
			SELECT MAX(NEW_CSOP_ID)
				INTO V_ID
				FROM REPGA_DETM_OBA_TBASE
			 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE;
			--
			INSERT /*+APPEND*/
			INTO REPGA_DETM_OBA_TBASE
				(SYM_RUN_DATE,
				 RUN_ID,
				 NEW_CSOP_ID,
				 AZONOSITO,
				 POSTAL_CODE,
				 CITY,
				 ADDRESS,
				 TIPUS,
				 SRC_SYS)
				SELECT P_SYM_RUN_DATE,
							 G_RUNID,
							 V_ID + ROWNUM,
							 F1 CLIENT_NO,
							 F25,
							 F26,
							 F27,
							 'M' TIPUS,
							 1 SRC_SYS
					FROM REPGA_DETM_OBA_BJEGY
				 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE;
			--
			COMMIT;
		END;

		--
		-- Urbis Clients
		--
		DECLARE
			V_ID NUMBER;
		BEGIN
			SELECT MAX(NEW_CSOP_ID)
				INTO V_ID
				FROM REPGA_DETM_OBA_TBASE
			 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE;
			--
			INSERT /*+APPEND*/
			INTO REPGA_DETM_OBA_TBASE
				(SYM_RUN_DATE,
				 RUN_ID,
				 NEW_CSOP_ID,
				 AZONOSITO,
				 POSTAL_CODE,
				 CITY,
				 ADDRESS,
				 TIPUS,
				 SRC_SYS)
				SELECT P_SYM_RUN_DATE,
							 G_RUNID,
							 V_ID + ROWNUM,
							 CLIENT_NO,
							 (SELECT IRSZ
									FROM REPGA_DETM_OBA_U_CLIENT
								 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
									 AND CLIENT_NO = X.CLIENT_NO
									 AND SZL IS NULL) IRSZ,
							 (SELECT CIMSOR1_2
									FROM REPGA_DETM_OBA_U_CLIENT
								 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
									 AND CLIENT_NO = X.CLIENT_NO
									 AND SZL IS NULL) CIMSOR1,
							 (SELECT CIMSOR3_5
									FROM REPGA_DETM_OBA_U_CLIENT
								 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
									 AND CLIENT_NO = X.CLIENT_NO
									 AND SZL IS NULL) CIMSOR2,
							 'M' TIPUS,
							 2 SRC_SYS
					FROM (SELECT DISTINCT CLIENT_NO
									FROM REPGA_DETM_OBA_U_CLIENT
								 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE) X;
			--
			COMMIT;
		END;

		--
		-- Értékjegy Clients
		--
		DECLARE
			V_ID NUMBER;
		BEGIN
			SELECT MAX(NEW_CSOP_ID)
				INTO V_ID
				FROM REPGA_DETM_OBA_TBASE
			 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE;
			--
			INSERT /*+APPEND*/
			INTO REPGA_DETM_OBA_TBASE
				(SYM_RUN_DATE,
				 RUN_ID,
				 NEW_CSOP_ID,
				 AZONOSITO,
				 POSTAL_CODE,
				 CITY,
				 ADDRESS,
				 TIPUS,
				 SRC_SYS)
				SELECT P_SYM_RUN_DATE,
							 G_RUNID,
							 V_ID + ROWNUM,
							 CLIENT_NO,
							 (SELECT F21
									FROM REPGA_DETM_OBA_EJEGY
								 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
									 AND F1 = X.CLIENT_NO
									 AND ROWNUM = 1) IRSZ,
							 (SELECT F22
									FROM REPGA_DETM_OBA_EJEGY
								 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
									 AND F1 = X.CLIENT_NO
									 AND ROWNUM = 1) CIMSOR1,
							 (SELECT F23
									FROM REPGA_DETM_OBA_EJEGY
								 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
									 AND F1 = X.CLIENT_NO
									 AND ROWNUM = 1) CIMSOR2,
							 'M' TIPUS,
							 3 SRC_SYS
					FROM (SELECT DISTINCT F1 CLIENT_NO
									FROM REPGA_DETM_OBA_EJEGY
								 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE) X;
			--
			COMMIT;
		END;

		--
		-- TÁRSASHÁZI ÜGYFELEK
		--
		UPDATE REPGA_DETM_OBA_TBASE X
			 SET TIPUS         = 'T',
					 LAKASOK_SZAMA = (SELECT LAKASOK_SZAMA
															FROM REPGA_DETM_OBA_TBASE_LAK
														 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
															 AND AZONOSITO = X.AZONOSITO)
		 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
			 AND AZONOSITO IN
					 (SELECT AZONOSITO FROM REPGA_DETM_OBA_TBASE_LAK WHERE SYM_RUN_DATE = P_SYM_RUN_DATE);
		COMMIT;

		DELETE FROM REPGA_DETM_OBA_TBASE -- ? 4_cup ?
		 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
			 AND TIPUS IS NULL;
		COMMIT;

		--EXECUTE IMMEDIATE 'CREATE INDEX cidx_OBA_TBASE ON OBA_TBASE(azonosito)';
		--EXECUTE IMMEDIATE 'CREATE INDEX idx_OBA_TBASE ON OBA_TBASE(new_csop_id)';

		-- Állami cégek, vezetõ tisztviselõk
		DELETE FROM REPGA_DETM_OBA_TBASE
		 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
			 AND NEW_CSOP_ID IN (SELECT NEW_CSOP_ID
														 FROM REPGA_DETM_OBA_TBASE  X,
																	REPGA_DETM_OBA_QCONTC C
														WHERE X.SYM_RUN_DATE = P_SYM_RUN_DATE
															AND C.SYM_RUN_DATE = P_SYM_RUN_DATE
															AND CLIENT_NO = X.AZONOSITO);
		COMMIT;

		UPDATE REPGA_DETM_OBA_TBASE X
			 SET (EGYENLEG, KAMAT, FORRASADO, LEJART, ZAROLT) = (SELECT SUM(BAL_HUF),
																																	SUM(INT_HUF),
																																	SUM(INT_TAX),
																																	SUM(MATURED),
																																	SUM(REST)
																														 FROM REPGA_DETM_OBA_SY_C
																														WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
																															AND CLIENT_NO = X.AZONOSITO)
		 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
			 AND SRC_SYS = '0' -- SY
		;
		COMMIT;

		UPDATE REPGA_DETM_OBA_TBASE X
			 SET (EGYENLEG, KAMAT, FORRASADO, ZAROLT) = (SELECT SUM(EGYENLEG),
																													SUM(KAMAT),
																													SUM(FORRASADO),
																													SUM(ZAROLT)
																										 FROM REPGA_DETM_OBA_U_BETET
																										WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
																											AND CLIENT_NO = X.AZONOSITO)
		 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
			 AND SRC_SYS = '2' -- Urbis
		;
		COMMIT;

		INSERT INTO REPGA_TMPM_OBA_BJEGY_N
			SELECT CLIENT_NO,
						 ROUND(SUM(EGYENLEG),
									 0) EGYENLEG,
						 ROUND(SUM(KAMAT),
									 0) KAMAT,
						 ROUND(SUM(FORRASADO),
									 0) FORRASADO,
						 ROUND(SUM(ZAROLT),
									 0) ZAROLT
				FROM (
							-- Bjegy
							SELECT F1 CLIENT_NO,
											EGYENLEG,
											KAMAT,
											FORRASADO,
											ZAROLT
								FROM REPGA_DETM_OBA_BJEGY
							 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE)
			 GROUP BY CLIENT_NO;

		--EXECUTE IMMEDIATE 'CREATE UNIQUE INDEX uidx_OBA_Bjegy_N ON OBA_Bjegy_N(client_no)';

		UPDATE REPGA_DETM_OBA_TBASE X
			 SET (EGYENLEG, KAMAT, FORRASADO, ZAROLT) = (SELECT SUM(EGYENLEG),
																													SUM(KAMAT),
																													SUM(FORRASADO),
																													SUM(ZAROLT)
																										 FROM REPGA_TMPM_OBA_BJEGY_N
																										WHERE CLIENT_NO = X.AZONOSITO)
		 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
			 AND SRC_SYS = '1' -- Betéti
		;
		COMMIT;

		-- Betéti rendszeren belül (férj és feleség ugyanazon azonosítóval rendelkezik- nyilván közös a betét)
		DECLARE
		BEGIN
			FOR REC IN (SELECT AZONOSITO,
												 COUNT(1) CNT
										FROM REPGA_DETM_OBA_TBASE
									 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
										 AND SRC_SYS = 1
									 GROUP BY AZONOSITO
									HAVING COUNT(1) > 1)
			LOOP
				--
				UPDATE REPGA_DETM_OBA_TBASE
					 SET EGYENLEG  = ROUND(EGYENLEG / REC.CNT),
							 KAMAT     = ROUND(KAMAT / REC.CNT),
							 FORRASADO = ROUND(FORRASADO / REC.CNT),
							 ZAROLT    = ROUND(ZAROLT / REC.CNT)
				 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
					 AND SRC_SYS = '1' -- Betéti
					 AND AZONOSITO = REC.AZONOSITO;
				--
			END LOOP;
			COMMIT;
		END;

		INSERT INTO REPGA_TMPM_OBA_EJEGY_N
			SELECT CLIENT_NO,
						 ROUND(SUM(EGYENLEG),
									 0) EGYENLEG,
						 ROUND(SUM(KAMAT),
									 0) KAMAT,
						 ROUND(SUM(FORRASADO),
									 0) FORRASADO,
						 ROUND(SUM(ZAROLT),
									 0) ZAROLT
				FROM (
							-- Bjegy
							SELECT F1 CLIENT_NO,
											EGYENLEG,
											KAMAT,
											FORRASADO,
											ZAROLT
								FROM REPGA_DETM_OBA_EJEGY
							 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE)
			 GROUP BY CLIENT_NO;

		--EXECUTE IMMEDIATE 'CREATE UNIQUE INDEX uidx_OBA_Ejegy_N ON OBA_Ejegy_N(client_no)';

		UPDATE REPGA_DETM_OBA_TBASE X
			 SET (EGYENLEG, KAMAT, FORRASADO, ZAROLT) = (SELECT SUM(EGYENLEG),
																													SUM(KAMAT),
																													SUM(FORRASADO),
																													SUM(ZAROLT)
																										 FROM REPGA_TMPM_OBA_EJEGY_N
																										WHERE CLIENT_NO = X.AZONOSITO)
		 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
			 AND SRC_SYS = '3' -- Ejegy
		;
		COMMIT;

    SELECT COUNT(*) INTO G_PROCESSED
    FROM REPGA_DETM_OBA_TBASE
    WHERE SYM_RUN_DATE = P_SYM_RUN_DATE;

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
		EXECUTE IMMEDIATE 'TRUNCATE TABLE REPGA_TMPM_OBA_BJEGY_N';
		EXECUTE IMMEDIATE 'TRUNCATE TABLE REPGA_TMPM_OBA_EJEGY_N';
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
