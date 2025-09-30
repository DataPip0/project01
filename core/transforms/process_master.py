import pandas as pd
from datetime import datetime
from ..logger import get_logger
logger = get_logger(__name__)
class ProcessMasterBuilder:
    def __init__(self, df):
        self.df = df.copy()
        self.stage_master = pd.DataFrame(); self.application_master = pd.DataFrame()
        self.build_stage_master(); self.build_application_master()
    def build_stage_master(self):
        out=[]
        for (app,st),g in self.df.groupby(["Application_ID","Stage"]):
            try:
                s0,s1=g["Stage_Start_Timestamp"].min(), g["Stage_End_Timestamp"].max()
                tat=(s1-s0).total_seconds()/60 if pd.notnull(s0) and pd.notnull(s1) else None
                a0=self.df[self.df["Application_ID"]==app]["Activity_Timestamp"].min()
                age=(s1-a0).days if pd.notnull(s1) and pd.notnull(a0) else None
                out.append({
                    "Application_ID":app,"Stage":st,"Stage_Start":s0,"Stage_End":s1,
                    "TAT_Minutes":tat,"Age_Days":age,
                    "Risk_Grade":g["Risk_Grade"].iloc[-1] if "Risk_Grade" in g else None,
                    "UW_Decision":g["UW_Decision"].iloc[-1] if "UW_Decision" in g else None,
                    "Stage_Status":g["Status_After_Activity"].iloc[-1] if "Status_After_Activity" in g else None,
                    "Performed_By":", ".join(g["Performed_By"].dropna().unique()) if "Performed_By" in g else None,
                    "Issues_Count":0})
            except Exception as e: logger.error(f"[STAGE] Build fail {app},{st} | {e}")
        self.stage_master=pd.DataFrame(out); logger.info(f"[STAGE] rows={len(self.stage_master)}")
    def build_application_master(self):
        out=[]
        for app,g in self.df.groupby("Application_ID"):
            try:
                a0,a1=g["Activity_Timestamp"].min(), g["Activity_Timestamp"].max()
                tat=(a1-a0).total_seconds()/60 if pd.notnull(a0) and pd.notnull(a1) else None
                age=(datetime.now()-a0).days if pd.notnull(a0) else None
                out.append({
                    "Application_ID":app,"Product_Type":g["Product_Type"].iloc[0] if "Product_Type" in g else None,
                    "Channel":g["Channel"].iloc[0] if "Channel" in g else None,
                    "Application_Start":a0,"Application_End":a1,
                    "Total_TAT_Minutes":tat,"Age_Days":age,
                    "Final_Risk_Grade":g["Risk_Grade"].iloc[-1] if "Risk_Grade" in g else None,
                    "Final_UW_Decision":g["UW_Decision"].iloc[-1] if "UW_Decision" in g else None,
                    "Application_Status":g["Status_After_Activity"].iloc[-1] if "Status_After_Activity" in g else None,
                    "Performed_By":", ".join(g["Performed_By"].dropna().unique()) if "Performed_By" in g else None,
                    "Issues_Count":0})
            except Exception as e: logger.error(f"[APP] Build fail {app} | {e}")
        self.application_master=pd.DataFrame(out); logger.info(f"[APP] rows={len(self.application_master)}")
