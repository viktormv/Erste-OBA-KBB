CREATE OR REPLACE PACKAGE REPGA_DETM_OBA_RES_URB_U_PKG IS
  -- %version   0.1   2012.10.10
  -- %author   Vincze Csaba
  -- %usage loading package for stage area
  -- %intab tablelist:  REPGA_DETM_OBA_TBASE,
  --                    REPGA_DETM_OBA_FIDB_R,
  --                    REPGA_DETM_OBA_U_BETET,
  --                    REPGA_DETM_OBA_BJEGY
  --                    REPGA_DETM_OBA_EJEGY
  --
  -- %outtab tablelist: REPGA_DETM_OBA_RES_URB_URB_U
  PROCEDURE MAIN(P_SYM_RUN_DATE DATE DEFAULT EBH_PWM_PUBLIC_P.F_GETSYMRUNDATE);

END;
/
CREATE OR REPLACE PACKAGE BODY REPGA_DETM_OBA_RES_URB_U_PKG IS
  -- %version   0.1   2012.10.10
  -- %author   Vincze Csaba

  -- Procedure for calculating and loading data into REPGA_DETM_OBA_RES_URB_U table
  -- %param p_sym_run_data: symbols running time <br>
  --                in form of date type
  -- %raises NO
  -- %dynasql YES
  -- %autonom NO
  -- %dbmsout NO
  ------------------------------------------------------------------------
  --   0.1 Vincze Csaba        Intitial

   GC_APPNAME CONSTANT VARCHAR2(30) := 'REPGA_DETM_OBA_RES_URB_U_PKG';

   GC_TARGETTABLE CONSTANT VARCHAR2(30):='REPGA_DETM_OBA_RES_URB_U';

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

    TYPE T_SYM_RUN_DATE  IS TABLE OF REPGA_DETM_OBA_RES_URB_U.SYM_RUN_DATE%TYPE INDEX BY PLS_INTEGER;
    TYPE T_CLIENT_NO     IS TABLE OF REPGA_DETM_OBA_RES_URB_U.CLIENT_NO%TYPE INDEX BY PLS_INTEGER;
    TYPE T_TIPUS         IS TABLE OF REPGA_DETM_OBA_RES_URB_U.TIPUS%TYPE INDEX BY PLS_INTEGER;
    TYPE T_NEV           IS TABLE OF REPGA_DETM_OBA_RES_URB_U.NEV%TYPE INDEX BY PLS_INTEGER;
    TYPE T_SZULNEV       IS TABLE OF REPGA_DETM_OBA_RES_URB_U.SZULNEV%TYPE INDEX BY PLS_INTEGER;
    TYPE T_ANYANEV       IS TABLE OF REPGA_DETM_OBA_RES_URB_U.ANYANEV%TYPE INDEX BY PLS_INTEGER;
    TYPE T_SZUL_IDO      IS TABLE OF REPGA_DETM_OBA_RES_URB_U.SZUL_IDO%TYPE INDEX BY PLS_INTEGER;
    TYPE T_SZUL_HELY     IS TABLE OF REPGA_DETM_OBA_RES_URB_U.SZUL_HELY%TYPE INDEX BY PLS_INTEGER;
    TYPE T_REZIDENS      IS TABLE OF REPGA_DETM_OBA_RES_URB_U.REZIDENS%TYPE INDEX BY PLS_INTEGER;
    TYPE T_SZIGSZ        IS TABLE OF REPGA_DETM_OBA_RES_URB_U.SZIGSZ%TYPE INDEX BY PLS_INTEGER;
    TYPE T_UTLEVSZ       IS TABLE OF REPGA_DETM_OBA_RES_URB_U.UTLEVSZ%TYPE INDEX BY PLS_INTEGER;
    TYPE T_VEZ_ENG       IS TABLE OF REPGA_DETM_OBA_RES_URB_U.VEZ_ENG%TYPE INDEX BY PLS_INTEGER;
    TYPE T_CJK           IS TABLE OF REPGA_DETM_OBA_RES_URB_U.CJK%TYPE INDEX BY PLS_INTEGER;
    TYPE T_NYILV         IS TABLE OF REPGA_DETM_OBA_RES_URB_U.NYILV%TYPE INDEX BY PLS_INTEGER;
    TYPE T_KSH           IS TABLE OF REPGA_DETM_OBA_RES_URB_U.KSH%TYPE INDEX BY PLS_INTEGER;
    TYPE T_ADOAZ         IS TABLE OF REPGA_DETM_OBA_RES_URB_U.ADOAZ%TYPE INDEX BY PLS_INTEGER;
    TYPE T_ADOSZAM       IS TABLE OF REPGA_DETM_OBA_RES_URB_U.ADOSZAM%TYPE INDEX BY PLS_INTEGER;
    TYPE T_POSTAL_CODE   IS TABLE OF REPGA_DETM_OBA_RES_URB_U.POSTAL_CODE%TYPE INDEX BY PLS_INTEGER;
    TYPE T_CITY          IS TABLE OF REPGA_DETM_OBA_RES_URB_U.CITY%TYPE INDEX BY PLS_INTEGER;
    TYPE T_ADDRESS       IS TABLE OF REPGA_DETM_OBA_RES_URB_U.ADDRESS%TYPE INDEX BY PLS_INTEGER;
    TYPE T_PHONE         IS TABLE OF REPGA_DETM_OBA_RES_URB_U.PHONE%TYPE INDEX BY PLS_INTEGER;
    TYPE T_EMAIL         IS TABLE OF REPGA_DETM_OBA_RES_URB_U.EMAIL%TYPE INDEX BY PLS_INTEGER;
    TYPE T_EGYENLEG      IS TABLE OF REPGA_DETM_OBA_RES_URB_U.EGYENLEG%TYPE INDEX BY PLS_INTEGER;
    TYPE T_KAMAT         IS TABLE OF REPGA_DETM_OBA_RES_URB_U.KAMAT%TYPE INDEX BY PLS_INTEGER;
    TYPE T_ZAROLT        IS TABLE OF REPGA_DETM_OBA_RES_URB_U.ZAROLT%TYPE INDEX BY PLS_INTEGER;
    TYPE T_OBA_LIMIT     IS TABLE OF REPGA_DETM_OBA_RES_URB_U.OBA_LIMIT%TYPE INDEX BY PLS_INTEGER;
    TYPE T_FORRASADO     IS TABLE OF REPGA_DETM_OBA_RES_URB_U.FORRASADO%TYPE INDEX BY PLS_INTEGER;
    TYPE T_LEJART        IS TABLE OF REPGA_DETM_OBA_RES_URB_U.LEJART%TYPE INDEX BY PLS_INTEGER;
    TYPE T_CONS_DATA     IS TABLE OF REPGA_DETM_OBA_RES_URB_U.CONS_DATA%TYPE INDEX BY PLS_INTEGER;

    L_SYM_RUN_DATE    T_SYM_RUN_DATE;
    L_CLIENT_NO       T_CLIENT_NO;
    L_TIPUS           T_TIPUS;
    L_NEV             T_NEV;
    L_SZULNEV         T_SZULNEV;
    L_ANYANEV         T_ANYANEV;
    L_SZUL_IDO        T_SZUL_IDO;
    L_SZUL_HELY       T_SZUL_HELY;
    L_REZIDENS        T_REZIDENS;
    L_SZIGSZ          T_SZIGSZ;
    L_UTLEVSZ         T_UTLEVSZ;
    L_VEZ_ENG         T_VEZ_ENG;
    L_CJK             T_CJK;
    L_NYILV           T_NYILV;
    L_KSH             T_KSH;
    L_ADOAZ           T_ADOAZ;
    L_ADOSZAM         T_ADOSZAM;
    L_POSTAL_CODE     T_POSTAL_CODE;
    L_CITY            T_CITY;
    L_ADDRESS         T_ADDRESS;
    L_PHONE           T_PHONE;
    L_EMAIL           T_EMAIL;
    L_EGYENLEG        T_EGYENLEG;
    L_KAMAT           T_KAMAT;
    L_ZAROLT          T_ZAROLT;
    L_OBA_LIMIT       T_OBA_LIMIT;
    L_FORRASADO       T_FORRASADO;
    L_LEJART          T_LEJART;
    L_CONS_DATA       T_CONS_DATA;

      CURSOR HC IS(
                    SELECT B.SYM_RUN_DATE, client_no,
                           DECODE(s.tipus,'E','E',b.tipus) tipus,
                           nev, szulnev, anyanev, szul_ido, szul_hely, rezidens,
                           szigsz, utlevsz, vez_eng, cjk, NULL nyilv, NULL ksh, NULL adoaz, NULL adoszam,
                           postal_code, city, address,
                           NULL phone, NULL email,
                           r.egyenleg, r.kamat, r.zarolt, G_HUF_LIMIT*multiply oba_limit,
                           --30037000*multiply oba_limit,
                           r.forrasado, r.lejart, cons_data
                      FROM REPGA_DETM_OBA_TBASE b, REPGA_DETM_OBA_TBASE_RES r, REPGA_DETM_OBA_U_CLIENT s
                     WHERE B.SYM_RUN_DATE = R.SYM_RUN_DATE
                       AND b.new_csop_id = r.new_csop_id
                       AND b.src_sys = 2  -- URBIS
                       AND B.SYM_RUN_DATE = S.SYM_RUN_DATE
                       AND s.client_no = b.azonosito
                       AND s.rno = 1 -- Caused by multi Urbis record per client
                       AND B.SYM_RUN_DATE = P_SYM_RUN_DATE
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
                  L_CLIENT_NO,
                  L_TIPUS,
                  L_NEV,
                  L_SZULNEV,
                  L_ANYANEV,
                  L_SZUL_IDO,
                  L_SZUL_HELY,
                  L_REZIDENS,
                  L_SZIGSZ,
                  L_UTLEVSZ,
                  L_VEZ_ENG,
                  L_CJK,
                  L_NYILV,
                  L_KSH,
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
            INSERT INTO REPGA_DETM_OBA_RES_URB_U
                 (SYM_RUN_DATE,
                  CLIENT_NO,
                  TIPUS,
                  NEV,
                  SZULNEV,
                  ANYANEV,
                  SZUL_IDO,
                  SZUL_HELY,
                  REZIDENS,
                  SZIGSZ,
                  UTLEVSZ,
                  VEZ_ENG,
                  CJK,
                  NYILV,
                  KSH,
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
               L_CLIENT_NO(X),
               L_TIPUS(X),
               L_NEV(X),
               L_SZULNEV(X),
               L_ANYANEV(X),
               L_SZUL_IDO(X),
               L_SZUL_HELY(X),
               L_REZIDENS(X),
               L_SZIGSZ(X),
               L_UTLEVSZ(X),
               L_VEZ_ENG(X),
               L_CJK(X),
               L_NYILV(X),
               L_KSH(X),
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