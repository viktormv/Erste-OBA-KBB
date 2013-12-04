CREATE OR REPLACE PACKAGE REPGA_STAM_OBA_EJEGY_PKG IS
  -- %version   0.3   2012.12.19
  -- %author   Vincze Csaba
  -- %usage loading package for stage area
  -- %intab tablelist:  REPGA_EXTM_OBA_EJEGY
  --
  -- %outtab tablelist: REPGA_STAM_OBA_EJEGY
  PROCEDURE MAIN(P_SYM_RUN_DATE DATE DEFAULT EBH_PWM_PUBLIC_P.F_GETSYMRUNDATE);

END;
/
CREATE OR REPLACE PACKAGE BODY REPGA_STAM_OBA_EJEGY_PKG IS
  -- %version   0.1   2012.10.25
  -- %version   0.2   2012.12.10
  -- %version   0.3   2012.12.19
  -- %author   Vincze Csaba

  -- Procedure for calculating and loading data into REPGA_STAM_OBA_QCONTC table
  -- %param p_sym_run_data: symbols running time <br>
  --                in form of date type
  -- %raises NO
  -- %dynasql YES
  -- %autonom NO
  -- %dbmsout NO
  -- VERSIONS
  ------------------------------------------
  -- v0.1     VCS INITIAL
  -- v0.2     VCS GC_TARGETTABLE javitas
  -- v0.3     VCS oszlpsorrend valtoztatas
  
  GC_APPNAME CONSTANT VARCHAR2(30) := 'REPGA_STAM_OBA_EJEGY_PKG';

  GC_TARGETTABLE CONSTANT VARCHAR2(30) := 'REPGA_STAM_OBA_EJEGY';

  GC_CSVNAME CONSTANT VARCHAR2(50) := 'REPGA_EJEGY.csv';

  GC_CSVDIR CONSTANT VARCHAR2(200) := 'DIR_FROMMANPRC_TOREPGA';

  GC_CSVARCHDIR CONSTANT VARCHAR2(200) := 'DIR_ARCH_FROMMANPRC_TOREPGA';

  G_PROCESSED NUMBER := 0;

  G_RUNID NUMBER;

  G_COMMITPOINT NUMBER := 20000;

  G_BATCHSIZE NUMBER := 5000;

  ----------------------------------------
  -- %version   0.1   VCS Initial

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

    TYPE T_SYM_RUN_DATE IS TABLE OF REPGA_STAM_OBA_EJEGY.SYM_RUN_DATE%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F1 IS TABLE OF REPGA_STAM_OBA_EJEGY.F1%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F2 IS TABLE OF REPGA_STAM_OBA_EJEGY.F2%TYPE INDEX BY PLS_INTEGER;
    TYPE T_EGYENLEG IS TABLE OF REPGA_STAM_OBA_EJEGY.EGYENLEG%TYPE INDEX BY PLS_INTEGER;
    TYPE T_KAMAT IS TABLE OF REPGA_STAM_OBA_EJEGY.KAMAT%TYPE INDEX BY PLS_INTEGER;
    TYPE T_FORRASADO IS TABLE OF REPGA_STAM_OBA_EJEGY.FORRASADO%TYPE INDEX BY PLS_INTEGER;
    TYPE T_ZAROLT IS TABLE OF REPGA_STAM_OBA_EJEGY.ZAROLT%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F7 IS TABLE OF REPGA_STAM_OBA_EJEGY.F7%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F8 IS TABLE OF REPGA_STAM_OBA_EJEGY.F8%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F9 IS TABLE OF REPGA_STAM_OBA_EJEGY.F9%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F10 IS TABLE OF REPGA_STAM_OBA_EJEGY.F10%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F11 IS TABLE OF REPGA_STAM_OBA_EJEGY.F11%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F12 IS TABLE OF REPGA_STAM_OBA_EJEGY.F12%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F13 IS TABLE OF REPGA_STAM_OBA_EJEGY.F13%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F14 IS TABLE OF REPGA_STAM_OBA_EJEGY.F14%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F15 IS TABLE OF REPGA_STAM_OBA_EJEGY.F15%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F16 IS TABLE OF REPGA_STAM_OBA_EJEGY.F16%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F17 IS TABLE OF REPGA_STAM_OBA_EJEGY.F17%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F18 IS TABLE OF REPGA_STAM_OBA_EJEGY.F18%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F19 IS TABLE OF REPGA_STAM_OBA_EJEGY.F19%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F20 IS TABLE OF REPGA_STAM_OBA_EJEGY.F20%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F21 IS TABLE OF REPGA_STAM_OBA_EJEGY.F21%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F22 IS TABLE OF REPGA_STAM_OBA_EJEGY.F22%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F23 IS TABLE OF REPGA_STAM_OBA_EJEGY.F23%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F24 IS TABLE OF REPGA_STAM_OBA_EJEGY.F24%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F25 IS TABLE OF REPGA_STAM_OBA_EJEGY.F25%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F26 IS TABLE OF REPGA_STAM_OBA_EJEGY.F26%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F27 IS TABLE OF REPGA_STAM_OBA_EJEGY.F27%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F28 IS TABLE OF REPGA_STAM_OBA_EJEGY.F28%TYPE INDEX BY PLS_INTEGER;

    L_SYM_RUN_DATE  T_SYM_RUN_DATE;
    L_F1            T_F1;
    L_F2            T_F2;
    L_EGYENLEG      T_EGYENLEG;
    L_KAMAT         T_KAMAT;
    L_FORRASADO     T_FORRASADO;
    L_ZAROLT        T_ZAROLT;
    L_F7            T_F7;
    L_F8            T_F8;
    L_F9            T_F9;
    L_F10           T_F10;
    L_F11           T_F11;
    L_F12           T_F12;
    L_F13           T_F13;
    L_F14           T_F14;
    L_F15           T_F15;
    L_F16           T_F16;
    L_F17           T_F17;
    L_F18           T_F18;
    L_F19           T_F19;
    L_F20           T_F20;
    L_F21           T_F21;
    L_F22           T_F22;
    L_F23           T_F23;
    L_F24           T_F24;
    L_F25           T_F25;
    L_F26           T_F26;
    L_F27           T_F27;
    L_F28           T_F28;

    CURSOR HC IS(
      SELECT P_SYM_RUN_DATE,
             f1, f2, "egyenleg", "kamat", "forrasado", "zarolt",
             f7, f8, f9, f10, f11, f12, f13, f14,
             f15, f16, f17, f18, f19, f20, f21, f22, f23,
             f24, f25, f26, f27,
             REGEXP_REPLACE(f28,
                            '([[:cntrl:]])|(^\t)',
                            NULL)
        FROM REPGA_EXTM_OBA_EJEGY
                );

  BEGIN
    REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
    OPEN HC;
    LOOP
      REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION || ' ' || TO_CHAR(L_ROWNUMBER));
      L_SYM_RUN_DATE.DELETE;
      FETCH HC BULK COLLECT
        INTO L_SYM_RUN_DATE,
             L_F1,
             L_F2,
             L_EGYENLEG,
             L_KAMAT,
             L_FORRASADO,
             L_ZAROLT,
             L_F7,
             L_F8,
             L_F9,
             L_F10,
             L_F11,
             L_F12,
             L_F13,
             L_F14,
             L_F15,
             L_F16,
             L_F17,
             L_F18,
             L_F19,
             L_F20,
             L_F21,
             L_F22,
             L_F23,
             L_F24,
             L_F25,
             L_F26,
             L_F27,
             L_F28 LIMIT G_BATCHSIZE;
      L_LAST := L_SYM_RUN_DATE.COUNT;
      EXIT WHEN L_LAST = 0;
      L_ROWNUMBER := L_ROWNUMBER + L_LAST;

      FORALL X IN 1 .. L_LAST
        INSERT INTO REPGA_STAM_OBA_EJEGY
          (SYM_RUN_DATE,
           F1, F2, EGYENLEG, KAMAT,
           FORRASADO, ZAROLT, F7, F8, F9, F10, F11, F12, F13, F14,
           F15, F16, F17, F18, F19, F20, F21, F22, F23,
           F24, F25, F26, F27, F28,
           RUN_ID)
        VALUES
          ( L_SYM_RUN_DATE(X),
            L_F1(X),
            L_F2(X),
            L_EGYENLEG(X),
            L_KAMAT(X),
            L_FORRASADO(X),
            L_ZAROLT(X),
            L_F7(X),
            L_F8(X),
            L_F9(X),
            L_F10(X),
            L_F11(X),
            L_F12(X),
            L_F13(X),
            L_F14(X),
            L_F15(X),
            L_F16(X),
            L_F17(X),
            L_F18(X),
            L_F19(X),
            L_F20(X),
            L_F21(X),
            L_F22(X),
            L_F23(X),
            L_F24(X),
            L_F25(X),
            L_F26(X),
            L_F27(X),
            L_F28(X),
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
  PROCEDURE RENAME_DATAFILE(P_SYM_RUN_DATE DATE) IS
    C_ACCTION CONSTANT VARCHAR2(15) := 'RENAME_DATAFILE';
    ARCHIVE_NAME VARCHAR2(50);
  BEGIN
    REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
    ARCHIVE_NAME := SUBSTR(GC_CSVNAME,
                           0,
                           LENGTH(GC_CSVNAME) - 4) || '_' ||
                    TO_CHAR(P_SYM_RUN_DATE,
                            'YYYYMMDD') || '.csv';

    UTL_FILE.FRENAME(GC_CSVDIR,
                     GC_CSVNAME,
                     GC_CSVARCHDIR,
                     ARCHIVE_NAME,
                     TRUE);
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

    IF REPGA_UTIL_PKG.IS_FILE_AVAILABLE(GC_CSVDIR,
                                        GC_CSVNAME) THEN
      DELETE_TABLE(P_SYM_RUN_DATE);
      INSERT_TABLE(P_SYM_RUN_DATE);
      RENAME_DATAFILE(P_SYM_RUN_DATE);
    ELSE
      REPGA_BACKOFFICE_PKG.API_MSG(0,
                                   'CSV read error...');
    END IF;

    POSTPROCESS(P_SYM_RUN_DATE);
    REPGA_BACKOFFICE_PKG.END_API(G_PROCESSED);
  EXCEPTION
    WHEN OTHERS THEN
      REPGA_BACKOFFICE_PKG.END_ERROR_API(SQLCODE,
                                         SQLERRM);
  END;

END;
/
