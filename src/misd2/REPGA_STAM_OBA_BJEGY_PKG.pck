CREATE OR REPLACE PACKAGE REPGA_STAM_OBA_BJEGY_PKG IS
  -- %version   0.4   2012.12.17
  -- %author   Vincze Csaba
  -- %usage loading package for stage area
  -- %intab tablelist:  REPGA_EXTM_OBA_BJEGY
  --
  -- %outtab tablelist: REPGA_STAM_OBA_BJEGY
  PROCEDURE MAIN(P_SYM_RUN_DATE DATE DEFAULT EBH_PWM_PUBLIC_P.F_GETSYMRUNDATE);

END;
/
CREATE OR REPLACE PACKAGE BODY REPGA_STAM_OBA_BJEGY_PKG IS
  -- %version   0.1   2012.10.10
  -- %version   0.2   2012.12.04
  -- %version   0.3   2012.12.10
  -- %version   0.4   2012.12.17
  -- %author   Vincze Csaba

  -- Procedure for calculating and loading data into REPGA_STAM_OBA_QCONTC table
  -- %param p_sym_run_data: symbols running time <br>
  --                in form of date type
  -- %raises NO
  -- %dynasql YES
  -- %autonom NO
  -- %dbmsout NO
  -- %dbmsout NO
  -------------------------------------------------------------------------------------
  -- %version   0.1   VCS Initial
  -- %version   0.2   VCS csv javitas
  -- %version   0.3   VCS GC_TARGETTABLE javitas
  -- %version   0.4   VCS csv javitas

  GC_APPNAME CONSTANT VARCHAR2(30) := 'REPGA_STAM_OBA_BJEGY_PKG';

  GC_TARGETTABLE CONSTANT VARCHAR2(30) := 'REPGA_STAM_OBA_BJEGY';

  GC_SOURCETABLE CONSTANT VARCHAR2(30) := 'REPGA_EXTM_OBA_BJEGY';

  GC_CSVDIR CONSTANT VARCHAR2(200) := 'DIR_FROMBETETI_TOOBA';

  GC_CSVARCHDIR CONSTANT VARCHAR2(200) := 'DIR_ARCH_FROMMANPRC_TOREPGA';

  GC_CSVNAME CONSTANT VARCHAR2(50) := 'BETETIOBA.CSV';

  G_CSVNAME VARCHAR2(100) := '';

  G_PROCESSED NUMBER := 0;

  G_RUNID NUMBER;

  G_COMMITPOINT NUMBER := 20000;

  G_BATCHSIZE NUMBER := 5000;

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

    TYPE T_SYM_RUN_DATE IS TABLE OF REPGA_STAM_OBA_BJEGY.SYM_RUN_DATE%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F1 IS TABLE OF REPGA_STAM_OBA_BJEGY.F1%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F2 IS TABLE OF REPGA_STAM_OBA_BJEGY.F2%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F3 IS TABLE OF REPGA_STAM_OBA_BJEGY.F3%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F4 IS TABLE OF REPGA_STAM_OBA_BJEGY.F4%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F5 IS TABLE OF REPGA_STAM_OBA_BJEGY.F5%TYPE INDEX BY PLS_INTEGER;
    TYPE T_EGYENLEG IS TABLE OF REPGA_STAM_OBA_BJEGY.EGYENLEG%TYPE INDEX BY PLS_INTEGER;
    TYPE T_KAMAT IS TABLE OF REPGA_STAM_OBA_BJEGY.KAMAT%TYPE INDEX BY PLS_INTEGER;
    TYPE T_FORRASADO IS TABLE OF REPGA_STAM_OBA_BJEGY.FORRASADO%TYPE INDEX BY PLS_INTEGER;
    TYPE T_ZAROLT IS TABLE OF REPGA_STAM_OBA_BJEGY.ZAROLT%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F10 IS TABLE OF REPGA_STAM_OBA_BJEGY.F10%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F11 IS TABLE OF REPGA_STAM_OBA_BJEGY.F11%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F12 IS TABLE OF REPGA_STAM_OBA_BJEGY.F12%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F13 IS TABLE OF REPGA_STAM_OBA_BJEGY.F13%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F14 IS TABLE OF REPGA_STAM_OBA_BJEGY.F14%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F15 IS TABLE OF REPGA_STAM_OBA_BJEGY.F15%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F16 IS TABLE OF REPGA_STAM_OBA_BJEGY.F16%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F17 IS TABLE OF REPGA_STAM_OBA_BJEGY.F17%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F18 IS TABLE OF REPGA_STAM_OBA_BJEGY.F18%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F19 IS TABLE OF REPGA_STAM_OBA_BJEGY.F19%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F20 IS TABLE OF REPGA_STAM_OBA_BJEGY.F20%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F21 IS TABLE OF REPGA_STAM_OBA_BJEGY.F21%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F22 IS TABLE OF REPGA_STAM_OBA_BJEGY.F22%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F23 IS TABLE OF REPGA_STAM_OBA_BJEGY.F23%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F24 IS TABLE OF REPGA_STAM_OBA_BJEGY.F24%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F25 IS TABLE OF REPGA_STAM_OBA_BJEGY.F25%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F26 IS TABLE OF REPGA_STAM_OBA_BJEGY.F26%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F27 IS TABLE OF REPGA_STAM_OBA_BJEGY.F27%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F28 IS TABLE OF REPGA_STAM_OBA_BJEGY.F28%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F29 IS TABLE OF REPGA_STAM_OBA_BJEGY.F29%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F30 IS TABLE OF REPGA_STAM_OBA_BJEGY.F30%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F31 IS TABLE OF REPGA_STAM_OBA_BJEGY.F31%TYPE INDEX BY PLS_INTEGER;
    TYPE T_F32 IS TABLE OF REPGA_STAM_OBA_BJEGY.F32%TYPE INDEX BY PLS_INTEGER;

    L_SYM_RUN_DATE  T_SYM_RUN_DATE;
    L_F1            T_F1;
    L_F2            T_F2;
    L_F3            T_F3;
    L_F4            T_F4;
    L_F5            T_F5;
    L_EGYENLEG      T_EGYENLEG;
    L_KAMAT         T_KAMAT;
    L_FORRASADO     T_FORRASADO;
    L_ZAROLT        T_ZAROLT;
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
    L_F29           T_F29;
    L_F30           T_F30;
    L_F31           T_F31;
    L_F32           T_F32;

    CURSOR HC IS(
      SELECT P_SYM_RUN_DATE,
             f1, f2, f3, f4, f5, "egyenleg", "kamat",
             "forrasado", "zarolt", f10, f11, f12, f13, f14,
             f15, f16, f17, f18, f19, f20, f21, f22, f23,
             f24, f25, f26, f27, f28, f29, f30, f31,
             REGEXP_REPLACE(f32,
                            '([[:cntrl:]])|(^\t)',
                            NULL)
        FROM REPGA_EXTM_OBA_BJEGY
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
             L_F3,
             L_F4,
             L_F5,
             L_EGYENLEG,
             L_KAMAT,
             L_FORRASADO,
             L_ZAROLT,
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
             L_F28,
             L_F29,
             L_F30,
             L_F31,
             L_F32 LIMIT G_BATCHSIZE;
      L_LAST := L_SYM_RUN_DATE.COUNT;
      EXIT WHEN L_LAST = 0;
      L_ROWNUMBER := L_ROWNUMBER + L_LAST;

      FORALL X IN 1 .. L_LAST
        INSERT INTO REPGA_STAM_OBA_BJEGY
          (SYM_RUN_DATE,
           F1, F2, F3, F4, F5, EGYENLEG, KAMAT,
           FORRASADO, ZAROLT, F10, F11, F12, F13, F14,
           F15, F16, F17, F18, F19, F20, F21, F22, F23,
           F24, F25, F26, F27, F28, F29, F30, F31, F32,
           RUN_ID)
        VALUES
          ( L_SYM_RUN_DATE(X),
            L_F1(X),
            L_F2(X),
            L_F3(X),
            L_F4(X),
            L_F5(X),
            L_EGYENLEG(X),
            L_KAMAT(X),
            L_FORRASADO(X),
            L_ZAROLT(X),
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
            L_F29(X),
            L_F30(X),
            L_F31(X),
            L_F32(X),
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
    L_SYM_RUN_DATE VARCHAR2(8);
  BEGIN
    REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
    G_BATCHSIZE   := REPGA_UTIL_PKG.GET_BATCHSIZE(GC_TARGETTABLE,
                                                  G_BATCHSIZE);
    G_COMMITPOINT := REPGA_UTIL_PKG.GET_COMMITPOINT(GC_TARGETTABLE,
                                                    G_COMMITPOINT);
    REPGA_UTIL_PKG.CLEANUP(P_SYM_RUN_DATE => P_SYM_RUN_DATE,
                           P_APPNAME      => GC_APPNAME,
                           P_TARGETNAME   => GC_TARGETTABLE);

    L_SYM_RUN_DATE := TO_CHAR(P_SYM_RUN_DATE,'YYYYMMDD');
    G_CSVNAME := '\'||L_SYM_RUN_DATE||'\'||GC_CSVNAME;

    EXECUTE IMMEDIATE 'ALTER TABLE '||GC_SOURCETABLE||' LOCATION ('''||G_CSVNAME||''')';
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
                                        G_CSVNAME) THEN
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
