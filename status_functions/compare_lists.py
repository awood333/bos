 
    # def compare_lists(self):
        
    #     #compare the last rows
        
    #     filepath    = "F:\\COWS\\data\\daily_milk.ods"
    #     sheetname   = 'AM_wy'
    #     data        = pd.read_excel(filepath,sheet_name=sheetname, skiprows=3, engine='odf')
    #     dailymilk_lastcol = data.iloc[:,-1].copy().sort_values().reset_index(drop=True)
        
        
    #     # from status_groups (via status_grous)
    #     a = self.SG.group_A_ids .iloc[-1:,1:].values
    #     b = self.SG.group_B_ids .iloc[-1:,1:].values
    #     f = self.SG.fresh_ids   .iloc[-1:,1:].values

    #     # Convert to DataFrames and pad with NaN to match lengths
    #     a_df = pd.DataFrame(a).T
    #     b_df = pd.DataFrame(b).T
    #     f_df = pd.DataFrame(f).T

    #     # Concatenate along columns, padding with NaN where necessary
    #     status_groups = pd.concat([a_df, b_df, f_df], axis=1, ignore_index=True)        
    #     status_groups.columns=['a','b','f']

    #     status_groups_list1 = np.concatenate((a, b, f), axis=1)
    #     status_groups_list2 = pd.DataFrame(status_groups_list1).dropna(axis=1, how='all')
    #     status_groups_list3 = status_groups_list2.T
    #     status_groups_list = status_groups_list3.sort_values(by=status_groups_list3.columns[0]).reset_index(drop=True)
    #     status_groups_list.columns = ['sg_list']
        
    #     # from status_data
    #     m = pd.DataFrame(np.array(self.SD.milker_ids[-1]),  columns=['sd_milkers_id'])
        
    #     # concat
    #     self.milker_lists_comp  = pd.concat( [a_df, b_df, f_df, status_groups_list, dailymilk_lastcol,  m], axis=1)
        
        
        
        # if len(m) != len(status_groups_list):
        #     print("look at F:\\COWS\\data\\status\\milker_lists_comp.csv")
        # if len(m) == len(status_groups_list):
        #     print("lists agree")

        # return self.milker_lists_comp
    