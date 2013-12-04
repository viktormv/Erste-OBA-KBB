CREATE OR REPLACE PACKAGE REPGA_DETM_OBA_RES_U_PKG IS
  -- %version   0.1   2012.10.10
  -- %author   Vincze Csaba
  -- %usage loading package for stage area
  -- %intab tablelist:  REPGA_DETM_OBA_TBASE,
  --                    REPGA_DETM_OBA_TBASE_RES, 
  --                    REPGA_DETM_OBA_SY_C,
  --                    REPGA_DETM_OBA_U_CLIENT
  --                    REPGA_DETM_OBA_BJEGY
  --
  -- %outtab tablelist: REPGA_DETM_OBA_RES_U
  PROCEDURE MAIN(P_SYM_RUN_DATE DATE DEFAULT EBH_PWM_PUBLIC_P.F_GETSYMRUNDATE);

END;
/
CREATE OR REPLACE PACKAGE BODY REPGA_DETM_OBA_RES_U_PKG IS
  -- %version   0.1   2012.10.10
  -- %author   Vincze Csaba

  -- Procedure for calculating and loading data into REPGA_DETM_OBA_RES_U table
  -- %param p_sym_run_data: symbols running time <br>
  --                in form of date type
  -- %raises NO
  -- %dynasql YES
  -- %autonom NO
  -- %dbmsout NO
  ------------------------------------------------------------------------
  --   0.1 Vincze Csaba        Intitial

   GC_APPNAME CONSTANT VARCHAR2(30) := 'REPGA_DETM_OBA_RES_U_PKG';

   GC_TARGETTABLE CONSTANT VARCHAR2(30):='REPGA_DETM_OBA_RES_U';

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
      REPGA_UTIL_PKG.DELETE_TABLE( GC_TARGETTABLE, P_SYM_RUN_DATE );
   END;
   ----------------------------------------
   PROCEDURE INSERT_TABLE(P_SYM_RUN_DATE DATE) IS
      C_ACCTION CONSTANT VARCHAR2(8) := 'INSERT';
      L_ROWNUMBER NUMBER := 0;
      L_INDEX     NUMBER := 0;
      L_LAST      NUMBER;

      TYPE T_SYM_RUN_DATE        IS TABLE OF REPGA_DETM_OBA_RES_U.SYM_RUN_DATE%TYPE INDEX BY PLS_INTEGER;
      TYPE T_NEW_CSOP_ID         IS TABLE OF REPGA_DETM_OBA_RES_U.NEW_CSOP_ID%TYPE INDEX BY PLS_INTEGER;
      TYPE T_TIPUS               IS TABLE OF REPGA_DETM_OBA_RES_U.TIPUS%TYPE INDEX BY PLS_INTEGER;
      TYPE T_CLIENT_NAME         IS TABLE OF REPGA_DETM_OBA_RES_U.CLIENT_NAME%TYPE INDEX BY PLS_INTEGER;
      TYPE T_MAIDEN_NAME         IS TABLE OF REPGA_DETM_OBA_RES_U.MAIDEN_NAME%TYPE INDEX BY PLS_INTEGER;
      TYPE T_MOTHER_MAIDEN_NAME  IS TABLE OF REPGA_DETM_OBA_RES_U.MOTHER_MAIDEN_NAME%TYPE INDEX BY PLS_INTEGER;
      TYPE T_BIRTH_DATE          IS TABLE OF REPGA_DETM_OBA_RES_U.BIRTH_DATE%TYPE INDEX BY PLS_INTEGER;
      TYPE T_BIRTH_PLACE         IS TABLE OF REPGA_DETM_OBA_RES_U.BIRTH_PLACE%TYPE INDEX BY PLS_INTEGER;
      TYPE T_COUNTRY_CITIZEN     IS TABLE OF REPGA_DETM_OBA_RES_U.COUNTRY_CITIZEN%TYPE INDEX BY PLS_INTEGER;
      TYPE T_SZIGSZ              IS TABLE OF REPGA_DETM_OBA_RES_U.SZIGSZ%TYPE INDEX BY PLS_INTEGER;
      TYPE T_UTLEV               IS TABLE OF REPGA_DETM_OBA_RES_U.UTLEV%TYPE INDEX BY PLS_INTEGER;
      TYPE T_JOGSI               IS TABLE OF REPGA_DETM_OBA_RES_U.JOGSI%TYPE INDEX BY PLS_INTEGER;
      TYPE T_CEGJSZAM            IS TABLE OF REPGA_DETM_OBA_RES_U.CEGJSZAM%TYPE INDEX BY PLS_INTEGER;
      TYPE T_NYILV               IS TABLE OF REPGA_DETM_OBA_RES_U.NYILV%TYPE INDEX BY PLS_INTEGER;
      TYPE T_KSHF8               IS TABLE OF REPGA_DETM_OBA_RES_U.KSHF8%TYPE INDEX BY PLS_INTEGER;
      TYPE T_ADOAZ               IS TABLE OF REPGA_DETM_OBA_RES_U.ADOAZ%TYPE INDEX BY PLS_INTEGER;
      TYPE T_ADOSZAM             IS TABLE OF REPGA_DETM_OBA_RES_U.ADOSZAM%TYPE INDEX BY PLS_INTEGER;
      TYPE T_POSTAL_CODE         IS TABLE OF REPGA_DETM_OBA_RES_U.POSTAL_CODE%TYPE INDEX BY PLS_INTEGER;
      TYPE T_CITY                IS TABLE OF REPGA_DETM_OBA_RES_U.CITY%TYPE INDEX BY PLS_INTEGER;
      TYPE T_ADDRESS             IS TABLE OF REPGA_DETM_OBA_RES_U.ADDRESS%TYPE INDEX BY PLS_INTEGER;
      TYPE T_PHONE               IS TABLE OF REPGA_DETM_OBA_RES_U.PHONE%TYPE INDEX BY PLS_INTEGER;
      TYPE T_EMAIL               IS TABLE OF REPGA_DETM_OBA_RES_U.EMAIL%TYPE INDEX BY PLS_INTEGER;
      TYPE T_EGYENLEG            IS TABLE OF REPGA_DETM_OBA_RES_U.EGYENLEG%TYPE INDEX BY PLS_INTEGER;
      TYPE T_KAMAT               IS TABLE OF REPGA_DETM_OBA_RES_U.KAMAT%TYPE INDEX BY PLS_INTEGER;
      TYPE T_ZAROLT              IS TABLE OF REPGA_DETM_OBA_RES_U.ZAROLT%TYPE INDEX BY PLS_INTEGER;
      TYPE T_OBA_LIMIT           IS TABLE OF REPGA_DETM_OBA_RES_U.OBA_LIMIT%TYPE INDEX BY PLS_INTEGER;
      TYPE T_FORRASADO           IS TABLE OF REPGA_DETM_OBA_RES_U.FORRASADO%TYPE INDEX BY PLS_INTEGER;
      TYPE T_LEJART              IS TABLE OF REPGA_DETM_OBA_RES_U.LEJART%TYPE INDEX BY PLS_INTEGER;
      TYPE T_CONS_DATA           IS TABLE OF REPGA_DETM_OBA_RES_U.CONS_DATA%TYPE INDEX BY PLS_INTEGER;

    L_SYM_RUN_DATE           T_SYM_RUN_DATE;
    L_NEW_CSOP_ID            T_NEW_CSOP_ID;
    L_TIPUS                  T_TIPUS;
    L_CLIENT_NAME            T_CLIENT_NAME;
    L_MAIDEN_NAME            T_MAIDEN_NAME;
    L_MOTHER_MAIDEN_NAME     T_MOTHER_MAIDEN_NAME;
    L_BIRTH_DATE             T_BIRTH_DATE;
    L_BIRTH_PLACE            T_BIRTH_PLACE;
    L_COUNTRY_CITIZEN        T_COUNTRY_CITIZEN;
    L_SZIGSZ                 T_SZIGSZ;
    L_UTLEV                  T_UTLEV;
    L_JOGSI                  T_JOGSI;
    L_CEGJSZAM               T_CEGJSZAM;
    L_NYILV                  T_NYILV;
    L_KSHF8                  T_KSHF8;
    L_ADOAZ                  T_ADOAZ;
    L_ADOSZAM                T_ADOSZAM;
    L_POSTAL_CODE            T_POSTAL_CODE;
    L_CITY                   T_CITY;
    L_ADDRESS                T_ADDRESS;
    L_PHONE                  T_PHONE;
    L_EMAIL                  T_EMAIL;
    L_EGYENLEG               T_EGYENLEG;
    L_KAMAT                  T_KAMAT;
    L_ZAROLT                 T_ZAROLT;
    L_OBA_LIMIT              T_OBA_LIMIT;
    L_FORRASADO              T_FORRASADO;
    L_LEJART                 T_LEJART;
    L_CONS_DATA              T_CONS_DATA;

      CURSOR HC IS(
                    /*SELECT NULL "KBB", NULL "Típus", NULL "Név", NULL "Elozo név", NULL "Anyja neve", NULL "Születési ido", NULL "Születési hely", NULL "Nemzetiség",
                           NULL "Sz. az. sz.", NULL "Útlevélszám", NULL "Jogosítvány szám", NULL "Cégjegyzékszám", NULL "Nyilvántartási szám", NULL "KSH szám",
                           NULL "Adóazonosító jel", NULL "Adószám",
                           NULL "Irányítószám", NULL "Település", NULL "Cím", NULL "Telefonszám", NULL "E-mail cím",
                           NULL "Összes toke", NULL "Összes kamat", NULL "Zárolt betétek összege", NULL "OBA kártalanítási értékhatár",
                           NULL "Kamatjövedelemadó", NULL "Lejárt követelések", NULL "Konszolidált összeg"
                      FROM dual
                     WHERE ROWNUM < 1
                    UNION ALL*/
                    SELECT b.SYM_RUN_DATE, r.new_csop_id,
                           DECODE(ugyftipus,'E','E',tipus) tipus,
                           client_name, maiden_name, mother_maiden_name, birth_date, birth_place, country_citizen,
                           szigsz, utlev, jogsi, cegjszam, NULL nyilv,
                           SUBSTR(ksh,1,8) kshF8,
                           adoaz, adoszam,
                           postal_code, city, address, REPLACE(phone,';',',') phone, email,
                           r.egyenleg, r.kamat, r.zarolt, G_HUF_LIMIT*multiply oba_limit,
                           --30037000*multiply oba_limit,
                           r.forrasado, r.lejart, cons_data
                      FROM REPGA_DETM_OBA_TBASE b, REPGA_DETM_OBA_TBASE_RES r, REPGA_DETM_OBA_SY_C s
                     WHERE b.sym_run_date = r.sym_run_date
                       AND b.new_csop_id = r.new_csop_id
                       AND b.src_sys = 0  -- SY
                       AND b.sym_run_date = s.sym_run_date
                       AND S.client_no = b.azonosito
                       AND b.SYM_RUN_DATE = P_SYM_RUN_DATE
                    UNION ALL
                    SELECT b.SYM_RUN_DATE, r.new_csop_id,
                           DECODE(s.tipus,'E','E',b.tipus) tipus,
                           nev, szulnev, anyanev, szul_ido, szul_hely, rezidens,
                           szigsz, utlevsz, vez_eng, cjk, NULL nyilv, NULL ksh, NULL adoaz, NULL adoszam,
                           postal_code, city, address, NULL phone, NULL email,
                           r.egyenleg, r.kamat, r.zarolt, G_HUF_LIMIT*multiply oba_limit,
                           --30037000*multiply oba_limit,
                           r.forrasado, r.lejart, cons_data
                      FROM REPGA_DETM_OBA_TBASE b, REPGA_DETM_OBA_TBASE_RES r, REPGA_DETM_OBA_U_CLIENT s
                     WHERE b.sym_run_date = r.sym_run_date
                       AND b.new_csop_id = r.new_csop_id
                       AND b.src_sys = 2  -- URBIS
                       AND b.sym_run_date = s.sym_run_date
                       AND s.client_no = b.azonosito
                       AND s.rno = 1 -- Caused by multi Urbis record per client
                       AND b.SYM_RUN_DATE = P_SYM_RUN_DATE
                    UNION ALL
                    SELECT b.SYM_RUN_DATE, r.new_csop_id,
                           tipus,
                           f11 nev, f12 szulnev, f13 anyanev,
                      --     TO_DATE(nvl2(f14,SUBSTR(f14,1,4)||'.'||SUBSTR(f14,5,2)||'.'||SUBSTR(f14,7,2),NULL),'yyyy.mm.dd') szul_ido,
                           szul_ido,
                           f15 szul_hely, f16 rezidens,
                           f17 szigsz, f18 utlevsz, f19 vez_eng, NULL cjk, NULL nyilv, NULL ksh, f22 adoaz, f23 adoszam,
                           postal_code, city, address, NULL phone, NULL email,
                           r.egyenleg, r.kamat, r.zarolt, G_HUF_LIMIT*multiply oba_limit,
                           --30037000*multiply oba_limit,
                           r.forrasado, r.lejart, cons_data
                      FROM REPGA_DETM_OBA_TBASE b, REPGA_DETM_OBA_TBASE_RES r, REPGA_DETM_OBA_BJEGY s
                     WHERE b.sym_run_date = r.sym_run_date
                       AND b.new_csop_id = r.new_csop_id
                       AND b.src_sys = 1 -- BJEGY
                       AND b.sym_run_date = s.sym_run_date
                       AND s.f1 = b.azonosito
                       AND b.SYM_RUN_DATE = P_SYM_RUN_DATE
                    UNION ALL
                    SELECT b.SYM_RUN_DATE, r.new_csop_id,
                           tipus,
                           f7 nev, f8 szulnev, f9 anyanev,
                           f10 szul_ido,
                           f11 szul_hely, f12 rezidens,
                           f13 szigsz, f14 utlevsz, f15 vez_eng, f16 cjk, NULL nyilv, NULL ksh, NULL adoaz, f17 adoszam,
                           postal_code, city, address, NULL phone, NULL email,
                           r.egyenleg, r.kamat, r.zarolt, G_HUF_LIMIT*multiply oba_limit,
                           --30037000*multiply oba_limit,
                           r.forrasado, r.lejart, cons_data
                      FROM REPGA_DETM_OBA_TBASE b, REPGA_DETM_OBA_TBASE_RES r, REPGA_DETM_OBA_EJEGY s
                     WHERE b.sym_run_date = r.sym_run_date
                       AND b.new_csop_id = r.new_csop_id
                       AND b.src_sys = 3 -- EJEGY
                       AND b.sym_run_date = s.sym_run_date
                       AND s.f1 = b.azonosito
                       AND s.f2 = -- Caused by multi EJEGY record per client
                                 (
                                  SELECT MIN(f2)
                                    FROM REPGA_DETM_OBA_EJEGY
                                   WHERE f1 = s.f1
                                 )
                       AND b.SYM_RUN_DATE = P_SYM_RUN_DATE
                     );


   BEGIN
      REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
      OPEN HC;
      LOOP
         REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION || ' ' ||
                                         TO_CHAR(L_ROWNUMBER));
         L_SYM_RUN_DATE.DELETE;
         FETCH HC BULK COLLECT
             INTO L_SYM_RUN_DATE,
                  L_NEW_CSOP_ID,
                  L_TIPUS,
                  L_CLIENT_NAME,
                  L_MAIDEN_NAME,
                  L_MOTHER_MAIDEN_NAME,
                  L_BIRTH_DATE,
                  L_BIRTH_PLACE,
                  L_COUNTRY_CITIZEN,
                  L_SZIGSZ,
                  L_UTLEV,
                  L_JOGSI,
                  L_CEGJSZAM,
                  L_NYILV,
                  L_KSHF8,
                  L_ADOAZ,
                  L_ADOSZAM,
                  L_POSTAL_CODE,
                  L_CITY,
                  L_ADDRESS,
                  L_PHONE,
                  L_EMAIL,
                  L_EGYENLEG,
                  L_KAMAT,
                  L_ZAROLT,
                  L_OBA_LIMIT,
                  L_FORRASADO,
                  L_LEJART,
                  L_CONS_DATA
            LIMIT G_BATCHSIZE;
         L_LAST := L_SYM_RUN_DATE.COUNT;
         EXIT WHEN L_LAST = 0;
         L_ROWNUMBER := L_ROWNUMBER + L_LAST;

         FORALL X IN 1 .. L_LAST
            INSERT INTO REPGA_DETM_OBA_RES_U
                 (SYM_RUN_DATE,
                  NEW_CSOP_ID,
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
                  ADOAZ,
                  ADOSZAM,
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
              (L_SYM_RUN_DATE(X),
               L_NEW_CSOP_ID(X),
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
               L_ADOAZ(X),
               L_ADOSZAM(X),
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
      G_PROCESSED:= G_PROCESSED+L_ROWNUMBER;
   END;

   ----------------------------------------
   PROCEDURE PREPROCESS(P_SYM_RUN_DATE DATE) IS
      C_ACCTION CONSTANT VARCHAR2(10) := 'PREPROCESS';
   BEGIN
      REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
      G_BATCHSIZE   := REPGA_UTIL_PKG.GET_BATCHSIZE(GC_TARGETTABLE,  G_BATCHSIZE);
      G_COMMITPOINT := REPGA_UTIL_PKG.GET_COMMITPOINT(GC_TARGETTABLE,G_COMMITPOINT);
      REPGA_UTIL_PKG.CLEANUP(P_SYM_RUN_DATE => P_SYM_RUN_DATE,
                             P_APPNAME      => GC_APPNAME,
                             P_TARGETNAME   => GC_TARGETTABLE);

     G_HUF_LIMIT := CONV_AMT_MTH(P_SYM_RUN_DATE, 'EUR', GC_EUR_LIMIT, 'HUF');

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
      G_RUNID := REPGA_BACKOFFICE_PKG.START_API(P_APPNAME      => GC_APPNAME,
                                                P_SYM_RUN_DATE => P_SYM_RUN_DATE);
      G_PROCESSED :=0;
      PREPROCESS(P_SYM_RUN_DATE);
      DELETE_TABLE(P_SYM_RUN_DATE);
      INSERT_TABLE(P_SYM_RUN_DATE);
      POSTPROCESS(P_SYM_RUN_DATE);
      REPGA_BACKOFFICE_PKG.END_API(G_PROCESSED);
   EXCEPTION
      WHEN OTHERS THEN
         REPGA_BACKOFFICE_PKG.END_ERROR_API(SQLCODE,SQLERRM);
   END;

END;
/
