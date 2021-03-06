CREATE OR REPLACE PACKAGE REPGA_DETM_OBA_CLIENTS_PKG IS
	-- %version   0.2   2011.09.22
	-- %version   0.1   2011.08.22
	-- %author   D�rnyei L�szl�
	-- %usage loading package for stage area
	-- %intab tablelist:  REPGA_DETM_AGA
	--                    REPGA_DETM_DEALS_EXP
	--                    REPGA_DETM_FK_CCY
	-- %outtab tablelist: REPGA_DETM_OBA_CLIENTS
	PROCEDURE MAIN(P_SYM_RUN_DATE DATE DEFAULT EBH_PWM_PUBLIC_P.F_GETSYMRUNDATE);

END;
/
CREATE OR REPLACE PACKAGE BODY REPGA_DETM_OBA_CLIENTS_PKG IS
	-- %version   0.2   2011.09.22
	-- %version   0.1   2011.08.22
	-- %author   D�rnyei L�szl�

	-- Procedure for calculating and loading data into REPGA_DETM_OBA_CLIENTS table
	-- %param p_sym_run_data: symbols running time <br>
	--                in form of date type
	-- %raises NO
	-- %dynasql NO
	-- %autonom NO
	-- %dbmsout NO
	GC_APPNAME CONSTANT VARCHAR2(30) := 'REPGA_DETM_OBA_CLIENTS_PKG';

	GC_TARGETTABLE CONSTANT VARCHAR2(30) := 'REPGA_DETM_OBA_CLIENTS';

	G_PROCESSED NUMBER := 0;

	G_RUNID NUMBER;

	G_COMMITPOINT NUMBER := 20000;

	G_BATCHSIZE NUMBER := 5000;

	----------------------------------------
	-- %version   0.1   DL l�trehoz�s
	-- %version   0.2   DL no data found eset�n csak END_WARN-os legyen a f�ban

	PROCEDURE DELETE_TABLE(P_SYM_RUN_DATE DATE) IS
		C_ACCTION CONSTANT VARCHAR2(6) := 'DELETE';
		L_DB NUMBER;
	BEGIN
		REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
		SELECT COUNT(*) INTO L_DB FROM REPGA_STAM_AGA WHERE SYM_RUN_DATE = P_SYM_RUN_DATE;
		IF L_DB > 0 THEN
			REPGA_UTIL_PKG.DELETE_TABLE(GC_TARGETTABLE,
																	P_SYM_RUN_DATE);
		END IF;
	END;

	----------------------------------------
	PROCEDURE INSERT_TABLE(P_SYM_RUN_DATE DATE) IS
		C_ACCTION CONSTANT VARCHAR2(8) := 'INSERT';
		L_ROWNUMBER NUMBER := 0;
		L_INDEX     NUMBER := 0;
		L_LAST      NUMBER;

		L_EUR_CCY_RATE NUMBER;

		TYPE T_SYM_RUN_DATE IS TABLE OF REPGA_DETM_OBA_CLIENTS.SYM_RUN_DATE%TYPE INDEX BY PLS_INTEGER;
		TYPE T_CLIENT_NO IS TABLE OF REPGA_DETM_OBA_CLIENTS.CLIENT_NO%TYPE INDEX BY PLS_INTEGER;
		TYPE T_CUSTOMER_TYPE_SL_CODE IS TABLE OF REPGA_DETM_OBA_CLIENTS.CUSTOMER_TYPE_SL_CODE%TYPE INDEX BY PLS_INTEGER;
		TYPE T_AMOUNT_LCY IS TABLE OF REPGA_DETM_OBA_CLIENTS.AMOUNT_LCY%TYPE INDEX BY PLS_INTEGER;
		TYPE T_AMOUNT_GAR IS TABLE OF REPGA_DETM_OBA_CLIENTS.AMOUNT_GAR%TYPE INDEX BY PLS_INTEGER;
		TYPE T_AMOUNT_OBA_FEEBASE IS TABLE OF REPGA_DETM_OBA_CLIENTS.AMOUNT_OBA_FEEBASE%TYPE INDEX BY PLS_INTEGER;
		TYPE T_OVER_OBA_LIMIT_FLAG IS TABLE OF REPGA_DETM_OBA_CLIENTS.OVER_OBA_LIMIT_FLAG%TYPE INDEX BY PLS_INTEGER;

		L_SYM_RUN_DATE          T_SYM_RUN_DATE;
		L_CLIENT_NO             T_CLIENT_NO;
		L_CUSTOMER_TYPE_SL_CODE T_CUSTOMER_TYPE_SL_CODE;
		L_AMOUNT_LCY            T_AMOUNT_LCY;
		L_AMOUNT_GAR            T_AMOUNT_GAR;
		L_AMOUNT_OBA_FEEBASE    T_AMOUNT_OBA_FEEBASE;
		L_OVER_OBA_LIMIT_FLAG   T_OVER_OBA_LIMIT_FLAG;

		CURSOR HC IS(
			SELECT SYM_RUN_DATE,
						 CLIENT_NO,
						 CUSTOMER_TYPE_SL_CODE,
						 AMOUNT_LCY,
						 AMOUNT_GAR,
						 AMOUNT_OBA_FEEBASE,
						 CASE
							 WHEN AMOUNT_OBA_FEEBASE > 100000 * L_EUR_CCY_RATE THEN
								'Y'
							 ELSE
								'N'
						 END AS OVER_OBA_LIMIT_FLAG
				FROM (SELECT P_SYM_RUN_DATE AS SYM_RUN_DATE,
										 A.CLIENT_NO,
										 A.CUSTOMER_TYPE_SL_CODE,
										 A.AMOUNT_LCY,
										 B.AMOUNT_GAR,
										 ABS(A.AMOUNT_LCY) - NVL(B.AMOUNT_GAR,
																						 0) AS AMOUNT_OBA_FEEBASE
								FROM (SELECT CLIENT_NO,
														 CUSTOMER_TYPE_SL_CODE,
														 SUM(AMOUNT_LCY) AS AMOUNT_LCY
												FROM REPGA_DETM_DEALS_EXP
											 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
												 AND LEDGER_ACCOUNT_SL_CODE IN
														 ( --SELECT SAP_FOKONYV FROM REPGA_MAP_SAP_GL_CODE WHERE OBA_DEPOSIT_FLAG = 'Y')
															'H441200', 'H441100', 'H441300', 'H442100', 'H442200', 'H442300',
															'H443100', 'H443200', 'H443300', 'H443500', 'H444100', 'H444200',
															'H444300', 'H444400', 'H445100', 'H447110', 'H447210', 'H481100',
															'H492100', 'H492200', 'H492600', 'H492700', 'H493110', 'H495100',
															'H495200')
											 GROUP BY CLIENT_NO,
																CUSTOMER_TYPE_SL_CODE) A,
										 (SELECT CLIENT_NO,
														 SUM(AMOUNT_GAR) AS AMOUNT_GAR
												FROM REPGA_DETM_AGA
											 WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
											 GROUP BY CLIENT_NO) B
							 WHERE A.CLIENT_NO = B.CLIENT_NO(+)));
	BEGIN
		REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);

		SELECT CCY_RATE
			INTO L_EUR_CCY_RATE
			FROM REPGA_DETM_FK_CCY
		 WHERE CCY = 'EUR'
			 AND SYM_RUN_DATE = P_SYM_RUN_DATE;

		OPEN HC;
		LOOP
			REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION || ' ' || TO_CHAR(L_ROWNUMBER));
			L_SYM_RUN_DATE.DELETE;
			FETCH HC BULK COLLECT
				INTO L_SYM_RUN_DATE, L_CLIENT_NO, L_CUSTOMER_TYPE_SL_CODE, L_AMOUNT_LCY, L_AMOUNT_GAR, L_AMOUNT_OBA_FEEBASE, L_OVER_OBA_LIMIT_FLAG LIMIT G_BATCHSIZE;
			L_LAST := L_SYM_RUN_DATE.COUNT;
			EXIT WHEN L_LAST = 0;
			L_ROWNUMBER := L_ROWNUMBER + L_LAST;

			FORALL X IN 1 .. L_LAST
				INSERT INTO REPGA_DETM_OBA_CLIENTS
					(SYM_RUN_DATE,
					 CLIENT_NO,
					 CUSTOMER_TYPE_SL_CODE,
					 AMOUNT_LCY,
					 AMOUNT_GAR,
					 AMOUNT_OBA_FEEBASE,
					 OVER_OBA_LIMIT_FLAG,
					 RUN_ID)
				VALUES
					(L_SYM_RUN_DATE(X),
					 L_CLIENT_NO(X),
					 L_CUSTOMER_TYPE_SL_CODE(X),
					 L_AMOUNT_LCY(X),
					 L_AMOUNT_GAR(X),
					 L_AMOUNT_OBA_FEEBASE(X),
					 L_OVER_OBA_LIMIT_FLAG(X),
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
    --> v02
    WHEN NO_DATA_FOUND THEN BEGIN
    			REPGA_BACKOFFICE_PKG.API_MSG(0,
																	 'ORA-01403: no data found');
          REPGA_BACKOFFICE_PKG.END_API(G_PROCESSED);
          END;--< v02
		WHEN OTHERS THEN
			REPGA_BACKOFFICE_PKG.END_ERROR_API(SQLCODE,
																				 SQLERRM);
	END;

END;
/
