CREATE OR REPLACE PACKAGE REPGA_DETM_OBA_TBASE_RES_PKG IS
	-- %version   0.1   2012.10.25.
	-- %author   Dörnyei László
	-- %usage loading package for detail area
	-- %intab tablelist:  
	--                    REPGA_DETM_OBA_TBASE <BR/>
	--                    REPGA_DETM_OBA_FIDB_R <BR/>
	--                    OBA_URBIS_MHT <BR/>

	-- %outtab tablelist: REPGA_DETM_OBA_TBASE_RES

	PROCEDURE MAIN(P_SYM_RUN_DATE DATE DEFAULT EBH_PWM_PUBLIC_P.F_GETSYMRUNDATE);

END;
/
CREATE OR REPLACE PACKAGE BODY REPGA_DETM_OBA_TBASE_RES_PKG IS
	-- %version   0.1   2012.10.25.
	-- %author   Dörnyei László
	--
	-- Procedure for calculating and loading data into REPGA_DETM_OBA_TBASE_RES table
	-- %param p_sym_run_data: symbols running time <br>
	--              in form of date type
	-- %raises NO
	-- %dynasql YES
	-- %autonom NO
	-- %dbmsout NO
	----------------------------------------------------------------------------------
	-- %version   0.1   DL ERSTEOBA-3 INITIAL

	GC_APPNAME CONSTANT VARCHAR2(30) := 'REPGA_DETM_OBA_TBASE_RES_PKG';

	GC_TARGETTABLE CONSTANT VARCHAR2(30) := 'REPGA_DETM_OBA_TBASE_RES';

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
	
		INSERT INTO REPGA_DETM_OBA_TBASE_RES
			(SYM_RUN_DATE,
			 RUN_ID,
			 NEW_CSOP_ID)
			SELECT DISTINCT P_SYM_RUN_DATE,
											G_RUNID,
											NEW_CSOP_ID
				FROM REPGA_DETM_OBA_TBASE
			 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE;
	
		UPDATE REPGA_DETM_OBA_TBASE_RES X
			 SET (EGYENLEG, KAMAT, FORRASADO, LEJART, ZAROLT) = (SELECT SUM(EGYENLEG),
																																	SUM(KAMAT),
																																	SUM(FORRASADO),
																																	SUM(LEJART),
																																	SUM(ZAROLT)
																														 FROM REPGA_DETM_OBA_TBASE
																														WHERE NEW_CSOP_ID = X.NEW_CSOP_ID
																															AND SYM_RUN_DATE = P_SYM_RUN_DATE)
		 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE;
		-- restraint max up 4 balance
		UPDATE REPGA_DETM_OBA_TBASE_RES
			 SET ZAROLT = EGYENLEG
		 WHERE EGYENLEG < ZAROLT
			 AND SYM_RUN_DATE = P_SYM_RUN_DATE;
	
		UPDATE REPGA_DETM_OBA_TBASE_RES X
			 SET MULTIPLY = (SELECT LAKASOK_SZAMA
												 FROM REPGA_DETM_OBA_TBASE
												WHERE NEW_CSOP_ID = X.NEW_CSOP_ID
													AND SYM_RUN_DATE = P_SYM_RUN_DATE)
		 WHERE NEW_CSOP_ID IN (SELECT NEW_CSOP_ID
														 FROM REPGA_DETM_OBA_TBASE
														WHERE LAKASOK_SZAMA IS NOT NULL
															AND SYM_RUN_DATE = P_SYM_RUN_DATE)
			 AND SYM_RUN_DATE = P_SYM_RUN_DATE;
		UPDATE REPGA_DETM_OBA_TBASE_RES
			 SET MULTIPLY = 1
		 WHERE MULTIPLY IS NULL
			 AND SYM_RUN_DATE = P_SYM_RUN_DATE;
		COMMIT;
	
		-- Ha a hitelintézet más hitelintézettõl betétállományt vett át, akkor fel kell tüntetnie az adatközlés megfelelõ sorában, 
		-- hogy a betétállomány átruházás idõpontja elõtt melyik hitelintézetnél helyezték el. (Hivatkozás: Hpt. 101. § (8) bek.) 
		-- fenti esetben a betétes ? a Hpt-ben meghatározott idõtartamig - külön-külön jogosult kártalanításra a mindenkori értékhatár szerint. 
		-- ING :: 2011.év December végén jár le.
	
		UPDATE REPGA_DETM_OBA_TBASE_RES
			 SET MULTIPLY = 0
		 WHERE P_SYM_RUN_DATE < DATE
		 '2012-01-01'
			 AND NEW_CSOP_ID IN (SELECT NEW_CSOP_ID
														 FROM REPGA_DETM_OBA_TBASE
														WHERE SRC_SYS = 0
															AND SYM_RUN_DATE = P_SYM_RUN_DATE
															AND AZONOSITO IN (SELECT CLIENT_NO
																									FROM (SELECT DISTINCT CLIENT_NO,
																																				ING
																													FROM REPGA_DETM_OBA_FIDB_R
																												 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
																													 AND CLIENT_NO IN
																															 (SELECT CLIENT_NO
																																	FROM REPGA_DETM_OBA_FIDB_R
																																 WHERE ING = 'Y'
																																	 AND SYM_RUN_DATE = P_SYM_RUN_DATE))
																								 GROUP BY CLIENT_NO
																								HAVING COUNT(1) > 1));
		COMMIT;
	
		-- Ha örökösként szereplõ személynek az adott hitelintézetnél saját tulajdonú betétje is van, 
		-- akkor a saját betétjét és az örökölt betét(rész)eket a fenti idõpontokig külön kell számítani 
		-- (vagyis ez esetben az örököst a 100 000 EUR-nak megfelelõ forintösszeg annyiszor illetheti meg, ahány örökhagyótól örökölt betétet).
		-- Urbis Örökösök
	
		DECLARE
			V_OBA_LIMIT NUMBER;
		BEGIN
			FOR REC IN (SELECT CLIENT_NO,
												 COUNT(1) ADDED_MULTIPLY
										FROM (SELECT DISTINCT CLIENT_NO,
																					OLD_CLIENT_NO
														FROM REPGA_TMPM_OBA_URBIS_MHT
													 WHERE OLD_CLIENT_NO IS NOT NULL
														 AND CLIENT_NO != OLD_CLIENT_NO)
									 GROUP BY CLIENT_NO)
			LOOP
				--
				UPDATE REPGA_DETM_OBA_TBASE_RES
					 SET MULTIPLY = MULTIPLY + REC.ADDED_MULTIPLY
				 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
					 AND NEW_CSOP_ID IN (SELECT NEW_CSOP_ID
																 FROM REPGA_DETM_OBA_TBASE
																WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
																	AND SRC_SYS = 2
																	AND AZONOSITO = REC.CLIENT_NO);
				--
			END LOOP;
			COMMIT;
		END;
	
		DECLARE
			V_OBA_LIMIT NUMBER;
		BEGIN
			--
			--SELECT central_bank_rate*100000 
			--INTO   v_oba_limit 
			--FROM   OBA_ARF 
			--WHERE  ccy = 'EUR';
			SELECT KMDW.CONV_AMT_MTH(P_SYM_RUN_DATE,
															 'EUR',
															 100000,
															 'HUF')
				INTO V_OBA_LIMIT
				FROM DUAL;
		
			--
			-- Konszolidált adat = Bruttó összeg (tõke+kamat) ? Zárolt összeg ? Kamatadó ? Lejárt követelések
			--
			UPDATE REPGA_DETM_OBA_TBASE_RES
				 SET CONS_DATA = (V_OBA_LIMIT * MULTIPLY) - NVL(FORRASADO,
																												0) -
												 NVL(LEJART,
														 0),
						 EUR_LIMIT = 'Y'
			 WHERE NVL(EGYENLEG,
								 0) + NVL(KAMAT,
													0) - NVL(ZAROLT,
																	 0) >= V_OBA_LIMIT * MULTIPLY
				 AND MULTIPLY > 0
				 AND SYM_RUN_DATE = P_SYM_RUN_DATE;
			--  
			UPDATE REPGA_DETM_OBA_TBASE_RES
				 SET CONS_DATA = NVL(EGYENLEG,
														 0) + NVL(KAMAT,
																			0) - NVL(ZAROLT,
																							 0) - NVL(FORRASADO,
																												0) -
												 NVL(LEJART,
														 0)
			 WHERE NVL(EGYENLEG,
								 0) + NVL(KAMAT,
													0) - NVL(ZAROLT,
																	 0) < V_OBA_LIMIT * MULTIPLY
				 AND MULTIPLY > 0
				 AND SYM_RUN_DATE = P_SYM_RUN_DATE;
			--
			COMMIT;
			--
			-- ING 
			--
			FOR REC IN (SELECT CLIENT_NO,
												 (SELECT NEW_CSOP_ID
														FROM REPGA_DETM_OBA_TBASE
													 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
														 AND SRC_SYS = 0
														 AND AZONOSITO = CLIENT_NO) NEW_CSOP_ID,
												 COUNT(1) ADDED_MULTIPLY,
												 SUM(CONS_BASE) CONS_BASE
										FROM (SELECT CLIENT_NO,
																 ING,
																 LEAST(V_OBA_LIMIT,
																			 SUM(NVL(BAL_HUF,
																							 0) + NVL(INT_HUF,
																												0) - NVL(RESTRAINT,
																																 0))) CONS_BASE
														FROM REPGA_DETM_OBA_FIDB_R
													 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
														 AND CLIENT_NO IN (SELECT CLIENT_NO
																								 FROM REPGA_DETM_OBA_FIDB_R
																								WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
																									AND ING = 'Y')
													 GROUP BY CLIENT_NO,
																		ING)
									 GROUP BY CLIENT_NO)
			LOOP
				--
				-- ING 
				-- Konszolidált adat = Bruttó összeg (tõke+kamat) ? Zárolt összeg ? Kamatadó ? Lejárt követelések
				--
				UPDATE REPGA_DETM_OBA_TBASE_RES
					 SET CONS_DATA = (V_OBA_LIMIT * REC.ADDED_MULTIPLY) -
													 NVL(FORRASADO,
															 0) - NVL(LEJART,
																				0),
							 MULTIPLY  = MULTIPLY + REC.ADDED_MULTIPLY,
							 EUR_LIMIT = 'Y'
				 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
					 AND REC.CONS_BASE >= V_OBA_LIMIT * REC.ADDED_MULTIPLY
					 AND NEW_CSOP_ID = REC.NEW_CSOP_ID
					 AND MULTIPLY = 0;
				--   
				UPDATE REPGA_DETM_OBA_TBASE_RES
					 SET CONS_DATA = REC.CONS_BASE - NVL(FORRASADO,
																							 0) - NVL(LEJART,
																												0),
							 MULTIPLY  = MULTIPLY + REC.ADDED_MULTIPLY
				 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
					 AND REC.CONS_BASE < V_OBA_LIMIT * REC.ADDED_MULTIPLY
					 AND NEW_CSOP_ID = REC.NEW_CSOP_ID
					 AND MULTIPLY = 0;
				--
			END LOOP;
			--
			COMMIT;
			--
		END;
	
		UPDATE REPGA_DETM_OBA_TBASE_RES
			 SET CONS_DATA = 0
		 WHERE CONS_DATA < 0
			 AND SYM_RUN_DATE = P_SYM_RUN_DATE;
		COMMIT;
	
		--EXECUTE IMMEDIATE 'CREATE UNIQUE INDEX uidx_OBA_TBASE_RESULT ON REPGA_DETM_OBA_TBASE_RES(new_csop_id)';
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
