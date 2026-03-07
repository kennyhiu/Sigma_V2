from _sigma.api import SigmaAPI
from _api.config import load_config
import argparse

def main():
      parser = argparse.ArgumentParser(description="Sigma API Client")
      parser.add_argument("--config", help="Path to configuration file")
      args = parser.parse_args()
      config = load_config(args.config)
      base_url = config['base_url']
      client_id = config['client_id']
      client_secret = config['client_secret']   
      Sigma= SigmaAPI(base_url, client_id, client_secret)
      if Sigma.authenticate():
            print("Authentication successful!")
            teams = Sigma.get_all_teams()
            if isinstance(teams, list):
                  print(f"Total teams: {len(teams)}")
                  for team in teams:
                        team_name = team.get("name", "(unnamed)")
                        team_id = team.get("teamId")
                        print(f"Team: {team_name} (Id: {team_id})")
            members = Sigma.get_all_members()
            if isinstance(members, list):
                  print(f"Total members: {len(members)}")
                  for member in members:
                        member_name = member.get("name", "(unnamed)")
                        member_id = member.get("memberId")
                        print(f"Member: {member_name} (Id: {member_id}) First Name: {member.get('firstName')} Last Name: {member.get('lastName')} Email: {member.get('email')}")
            else:
                  print("Failed to retrieve members.")
            workbooks = Sigma.get_all_workbooks()
            if isinstance(workbooks, list):
                  print(f"Total workbooks: {len(workbooks)}")
                  for workbook in workbooks:
                        workbook_name = workbook.get("name", "(unnamed)")
                        workbook_id = workbook.get("workbookId")
                        workbook_urlid = workbook.get("workbookUrlId")
                        if not workbook_id:
                              print(f"Workbook: {workbook_name} (ID: missing)")
                              continue

                        print(f"Workbook: {workbook_name} (Id: {workbook_id}, UrlId: {workbook_urlid})")
                        tags = Sigma.get_workbook_tags(workbook_urlid) or []
                        #print(f"Tags for workbook '{workbook_name}' (UrlId: {workbook_urlid})")
                        versions = Sigma.get_workbook_version_history(workbook_urlid) or []
                        #print(f"Version history for workbook '{workbook_name}' (UrlId: {workbook_urlid}):")
                        latest_version = None
                        for version in versions:
                              version_number = version.get("version")
                              published_by = version.get('publishedBy')
                              tags = version.get("tags", [])
                              for tag in tags:
                                    tag_id = tag.get("versionTagId")
                                    tagged_by = tag.get("taggedBy")
                                    tag_name = Sigma.get_tag_name(tag_id)
                                    print(f"Tag Id: {tag_id} ({tag_name}) (Tagged By: {tagged_by})")
                                    
                              if version_number is not None:
                                    if latest_version is None or version_number > latest_version.get("version", -1):
                                          latest_version = version
                        if latest_version:
                              print(f"Latest version: {latest_version.get('version')} published by {latest_version.get('publishedBy')}")

                              
            
      else:
            print("Authentication failed.")
      

    
if __name__ == "__main__":    
     main()


    
