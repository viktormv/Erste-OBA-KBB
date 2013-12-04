CREATE OR REPLACE PACKAGE REPGA_DETM_OBA_RES_DOLG_UY_PKG IS
	-- %version   0.1   2012.10.11
	-- %author   D�rnyei L�szl�
	-- %usage loading package for stage area
	-- %intab tablelist:  REPGA_DETM_OBA_TBASE,
	--                    REPGA_DETM_OBA_TBASE_RES,
	--                    REPGA_DETM_OBA_SY_C
	--
	-- %outtab tablelist: REPGA_DETM_OBA_RES_DOLG_UY
	PROCEDURE MAIN(P_SYM_RUN_DATE DATE DEFAULT EBH_PWM_PUBLIC_P.F_GETSYMRUNDATE);

END;
/
CREATE OR REPLACE PACKAGE BODY REPGA_DETM_OBA_RES_DOLG_UY_PKG IS
	-- %version   0.1   2012.10.11
	-- %author   D�rnyei L�szl�

	-- Procedure for calculating and loading data into REPGA_DETM_OBA_RES_DOLG_UY table
	-- %param p_sym_run_data: symbols running time <br>
	--                in form of date type
	-- %raises NO
	-- %dynasql YES
	-- %autonom NO
	-- %dbmsout NO
	------------------------------------------------------------------------
	-- %version   0.1   DL ERSTEOBA-3 INITIAL

	GC_APPNAME CONSTANT VARCHAR2(30) := 'REPGA_DETM_OBA_RES_DOLG_UY_PKG';

	GC_TARGETTABLE CONSTANT VARCHAR2(30) := 'REPGA_DETM_OBA_RES_DOLG_UY';

	GC_EUR_LIMIT NUMBER := 100000; -- OBA limit 100 000.- EUR

	G_HUF_LIMIT NUMBER := 0;

	G_PROCESSED NUMBER := 0;

	G_RUNID NUMBER;

	G_COMMITPOINT NUMBER := 200000;

	G_BATCHSIZE NUMBER := 5000;

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
		C_ACCTION CONSTANT VARCHAR2(8) := 'INSERT';
		L_ROWNUMBER NUMBER := 0;
		L_INDEX     NUMBER := 0;
		L_LAST      NUMBER;
	
		--TYPE T_SYM_RUN_DATE        IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.SYM_RUN_DATE%TYPE INDEX BY PLS_INTEGER;
		TYPE T_NEW_CSOP_ID IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.NEW_CSOP_ID%TYPE INDEX BY PLS_INTEGER;
		TYPE T_DOLG IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.DOLG%TYPE INDEX BY PLS_INTEGER;
		TYPE T_TIPUS IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.TIPUS%TYPE INDEX BY PLS_INTEGER;
		TYPE T_CLIENT_NAME IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.CLIENT_NAME%TYPE INDEX BY PLS_INTEGER;
		TYPE T_MAIDEN_NAME IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.MAIDEN_NAME%TYPE INDEX BY PLS_INTEGER;
		TYPE T_MOTHER_MAIDEN_NAME IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.MOTHER_MAIDEN_NAME%TYPE INDEX BY PLS_INTEGER;
		TYPE T_BIRTH_DATE IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.BIRTH_DATE%TYPE INDEX BY PLS_INTEGER;
		TYPE T_BIRTH_PLACE IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.BIRTH_PLACE%TYPE INDEX BY PLS_INTEGER;
		TYPE T_COUNTRY_CITIZEN IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.COUNTRY_CITIZEN%TYPE INDEX BY PLS_INTEGER;
		TYPE T_SZIGSZ IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.SZIGSZ%TYPE INDEX BY PLS_INTEGER;
		TYPE T_UTLEV IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.UTLEV%TYPE INDEX BY PLS_INTEGER;
		TYPE T_JOGSI IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.JOGSI%TYPE INDEX BY PLS_INTEGER;
		TYPE T_CEGJSZAM IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.CEGJSZAM%TYPE INDEX BY PLS_INTEGER;
		TYPE T_NYILV IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.NYILV%TYPE INDEX BY PLS_INTEGER;
		TYPE T_KSHF8 IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.KSHF8%TYPE INDEX BY PLS_INTEGER;
		TYPE T_ACTDAT IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.ACTDAT%TYPE INDEX BY PLS_INTEGER;
		TYPE T_ADOAZ IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.ADOAZ%TYPE INDEX BY PLS_INTEGER;
		TYPE T_ADOSZAM IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.ADOSZAM%TYPE INDEX BY PLS_INTEGER;
		TYPE T_LOC_CODE IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.LOC_CODE%TYPE INDEX BY PLS_INTEGER;
		TYPE T_LOC_CITY IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.LOC_CITY%TYPE INDEX BY PLS_INTEGER;
		TYPE T_LOC_STREET IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.LOC_STREET%TYPE INDEX BY PLS_INTEGER;
		TYPE T_POSTAL_CODE IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.POSTAL_CODE%TYPE INDEX BY PLS_INTEGER;
		TYPE T_CITY IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.CITY%TYPE INDEX BY PLS_INTEGER;
		TYPE T_ADDRESS IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.ADDRESS%TYPE INDEX BY PLS_INTEGER;
		TYPE T_PHONE IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.PHONE%TYPE INDEX BY PLS_INTEGER;
		TYPE T_EMAIL IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.EMAIL%TYPE INDEX BY PLS_INTEGER;
		TYPE T_EGYENLEG IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.EGYENLEG%TYPE INDEX BY PLS_INTEGER;
		TYPE T_KAMAT IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.KAMAT%TYPE INDEX BY PLS_INTEGER;
		TYPE T_ZAROLT IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.ZAROLT%TYPE INDEX BY PLS_INTEGER;
		TYPE T_OBA_LIMIT IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.OBA_LIMIT%TYPE INDEX BY PLS_INTEGER;
		TYPE T_FORRASADO IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.FORRASADO%TYPE INDEX BY PLS_INTEGER;
		TYPE T_LEJART IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.LEJART%TYPE INDEX BY PLS_INTEGER;
		TYPE T_CONS_DATA IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.CONS_DATA%TYPE INDEX BY PLS_INTEGER;
		--TYPE T_RUN_ID              IS TABLE OF REPGA_DETM_OBA_RES_DOLG_UY.RUN_ID%TYPE INDEX BY PLS_INTEGER;
	
		--L_SYM_RUN_DATE            T_SYM_RUN_DATE;
		L_NEW_CSOP_ID        T_NEW_CSOP_ID;
		L_DOLG               T_DOLG;
		L_TIPUS              T_TIPUS;
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
		L_NYILV              T_NYILV;
		L_KSHF8              T_KSHF8;
		L_ACTDAT             T_ACTDAT;
		L_ADOAZ              T_ADOAZ;
		L_ADOSZAM            T_ADOSZAM;
		L_LOC_CODE           T_LOC_CODE;
		L_LOC_CITY           T_LOC_CITY;
		L_LOC_STREET         T_LOC_STREET;
		L_POSTAL_CODE        T_POSTAL_CODE;
		L_CITY               T_CITY;
		L_ADDRESS            T_ADDRESS;
		L_PHONE              T_PHONE;
		L_EMAIL              T_EMAIL;
		L_EGYENLEG           T_EGYENLEG;
		L_KAMAT              T_KAMAT;
		L_ZAROLT             T_ZAROLT;
		L_OBA_LIMIT          T_OBA_LIMIT;
		L_FORRASADO          T_FORRASADO;
		L_LEJART             T_LEJART;
		L_CONS_DATA          T_CONS_DATA;
	
		CURSOR HC IS(
			SELECT CLIENT_NO,
						 DECODE(DOLG,
										'Y',
										'E',
										'P') CEG, -- Erste Too / Posta Only
						 DECODE(UGYFTIPUS,
										'E',
										'E',
										TIPUS) TIPUS,
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
						 NULL NYILV,
						 KSH,
						 P_SYM_RUN_DATE TARGY_HO,
						 ADOAZ,
						 ADOSZAM,
						 LOC_CODE,
						 LOC_CITY,
						 LOC_STREET,
						 POSTAL_CODE,
						 CITY,
						 ADDRESS,
						 PHONE,
						 EMAIL,
						 R.EGYENLEG,
						 R.KAMAT,
						 R.ZAROLT,
						 G_HUF_LIMIT * MULTIPLY OBA_LIMIT,
						 R.FORRASADO,
						 R.LEJART,
						 CONS_DATA
				FROM REPGA_DETM_OBA_TBASE     B,
						 REPGA_DETM_OBA_TBASE_RES R,
						 REPGA_DETM_OBA_SY_C      S
			 WHERE B.SYM_RUN_DATE = P_SYM_RUN_DATE
				 AND R.SYM_RUN_DATE = P_SYM_RUN_DATE
				 AND S.SYM_RUN_DATE = P_SYM_RUN_DATE
				 AND B.NEW_CSOP_ID = R.NEW_CSOP_ID
				 AND B.SRC_SYS = 0 -- SY
				 AND S.CLIENT_NO = B.AZONOSITO
				 AND DOLG IS NOT NULL);
	
	BEGIN
		REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
		OPEN HC;
		LOOP
			REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION || ' ' || TO_CHAR(L_ROWNUMBER));
			L_NEW_CSOP_ID.DELETE;
			FETCH HC BULK COLLECT
				INTO L_NEW_CSOP_ID, L_DOLG, L_TIPUS, L_CLIENT_NAME, L_MAIDEN_NAME, L_MOTHER_MAIDEN_NAME, L_BIRTH_DATE, L_BIRTH_PLACE, L_COUNTRY_CITIZEN, L_SZIGSZ, L_UTLEV, L_JOGSI, L_CEGJSZAM, L_NYILV, L_KSHF8, L_ACTDAT, L_ADOAZ, L_ADOSZAM, L_LOC_CODE, L_LOC_CITY, L_LOC_STREET, L_POSTAL_CODE, L_CITY, L_ADDRESS, L_PHONE, L_EMAIL, L_EGYENLEG, L_KAMAT, L_ZAROLT, L_OBA_LIMIT, L_FORRASADO, L_LEJART, L_CONS_DATA LIMIT G_BATCHSIZE;
			L_LAST := L_NEW_CSOP_ID.COUNT;
			EXIT WHEN L_LAST = 0;
			L_ROWNUMBER := L_ROWNUMBER + L_LAST;
		
			FORALL X IN 1 .. L_LAST
				INSERT INTO REPGA_DETM_OBA_RES_DOLG_UY
					(SYM_RUN_DATE,
					 NEW_CSOP_ID,
					 DOLG,
					 TIPUS,
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
					 NYILV,
					 KSHF8,
					 ACTDAT,
					 ADOAZ,
					 ADOSZAM,
					 LOC_CODE,
					 LOC_CITY,
					 LOC_STREET,
					 POSTAL_CODE,
					 CITY,
					 ADDRESS,
					 PHONE,
					 EMAIL,
					 EGYENLEG,
					 KAMAT,
					 ZAROLT,
					 OBA_LIMIT,
					 FORRASADO,
					 LEJART,
					 CONS_DATA,
					 RUN_ID)
				VALUES
					(P_SYM_RUN_DATE,
					 L_NEW_CSOP_ID(X),
					 L_DOLG(X),
					 L_TIPUS(X),
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
					 L_NYILV(X),
					 L_KSHF8(X),
					 L_ACTDAT(X),
					 L_ADOAZ(X),
					 L_ADOSZAM(X),
					 L_LOC_CODE(X),
					 L_LOC_CITY(X),
					 L_LOC_STREET(X),
					 L_POSTAL_CODE(X),
					 L_CITY(X),
					 L_ADDRESS(X),
					 L_PHONE(X),
					 L_EMAIL(X),
					 L_EGYENLEG(X),
					 L_KAMAT(X),
					 L_ZAROLT(X),
					 L_OBA_LIMIT(X),
					 L_FORRASADO(X),
					 L_LEJART(X),
					 L_CONS_DATA(X),
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
	
		G_HUF_LIMIT := CONV_AMT_MTH(P_SYM_RUN_DATE,
																'EUR',
																GC_EUR_LIMIT,
																'HUF');
	
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
