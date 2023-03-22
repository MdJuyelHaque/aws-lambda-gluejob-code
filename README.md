# SAFERA LAB-2

## Introduction

After the data sets have been prepared for analytical consumption, the next logical step is to visualize the data leveraging any commonly available data visualization tool like PowerBI. This lab will help the student understand the power of data visualizations and teach them techniques to enable it. 

The students will start this lab by importing the data as prepared in the previous lab into PowerBI
They will then work through the steps to visualize these data sets in various charts and graph widgets available in PowerBI
They can also perform some pivoting and other quick analytical functions available in PowerBI
The students will have a PowerBI dashboard that will help them visualize the data in its full glory.
An introduction to PowerBI would also be provided (for those unfamiliar with Microsoft PowerBI)

# Objectives

This lab will introduce you to Power BI, a widely-used data visualization tool. We will be designing visuals that utilize the output dataset which we stored in the Azure SQL database in Lab-1.

We have Successfully Completed Lab1 with transformation of Data. In Lab2 We are going to create Power BI Report and Visuals.

## Task-1:- Login to Power BI
1. **Open** [Power BI](https://app.powerbi.com) in your browser in **incognito mode** *(Google Chrome preferred)*<table><tr><td>
    <img src="https://user-images.githubusercontent.com/60775543/226533635-870db5ac-3ecb-461b-ab03-a7ecebad4e5a.png">
    </td></tr></table><br>

2.  **Enter Username** which is provided to you and **Click** on Submit.<br><table><tr><td>
<img src="https://user-images.githubusercontent.com/60775543/226537705-c86326c4-6160-413e-a1ed-54fef020bc63.png"></td></tr></table>





3. **Enter Password** and **Click** on Sign in.

<br><table><tr><td>
<img src="https://user-images.githubusercontent.com/60775543/226538381-4bc6e1a8-d1a7-450c-924a-e1f0b9f27ad5.png">
</td></tr></table>

4. You might see a prompt like the one pictured below, ***Click*** on Ask Later *(Note- Whenever you find this prompt click on the Ask Later option)*
<br><table><tr><td>
<img src="https://user-images.githubusercontent.com/125960918/223998689-8484c541-a214-4f6c-b43a-f627a609c454.png">
</td></tr></table>


Now you have successfully logged in to the Power BI service and have completed Task-1 for this lab.

## Task 2- Creating Power BI Visuals & Report.
### Section A - Connecting to Dataset.

Once you logged in to Power BI service, ***click*** om My Workspace.

<br><table><tr><td>
<img src="https://user-images.githubusercontent.com/60775543/226540972-e5f189a8-5ded-4506-ab94-e5da09f7ca60.png">
</td></tr></table>


You will find one report,dataset and a pbix file for reference that we have created already.

***Click*** on **Safera** Report.

<br><table><tr><td>
<img src="https://user-images.githubusercontent.com/60775543/226541508-d08a1920-27d6-4718-aa7b-c4b06c38ce62.png">
</td></tr></table>

A Report will open, let's talk about that.


  1. We have added **Year** as a filter. Use this filter to see data according to Year.
  2. We have added a visual to show **Incidents** by Type. ***Click*** on each bar and see data according to selected type.
<table><tr><td>
<img src="https://user-images.githubusercontent.com/60775543/226546360-59d76b12-224d-44aa-805d-ab49cb53e000.png">
</td></tr></table>

3. In this Visual We have added **Date in Between Filter**. Use this Filter to see data according to selected date range.
4. This visual shows **Count Of Incidents** as ***KPI***
5. Here we are showing number of **Arrests** among all Incidents. ***Click*** on any value and see data according to that.
   
<table><tr><td>
<img src="https://user-images.githubusercontent.com/60775543/226554290-81327ee6-ae2e-4b1a-ba96-d85ab1df9a5a.png">
</td></tr></table>

6. In this Visual We are showing Number of incidents by their location in Heat Map. 
<table><tr><td>
<img src="https://user-images.githubusercontent.com/60775543/226906737-edbfc99a-69e8-4d1b-9900-068e62d527ea.png">
</td></tr></table>
   
7. Here we are talking about anomalies. look at the sudden spike in visual with some points, that point indicated the anomaly. ***Click*** on any point it will explain the anomalies.
<table><tr><td>
<img src="https://user-images.githubusercontent.com/60775543/226554306-745634fe-8fce-45b2-b043-146f80d5242f.png">
</td></tr></table><br>

8. This is a **Matrix** Visual. Here we are showing the number of incidents by their location in a hierarchical manner. At the top level, there is **Community Area**, by expanding Community Area you will see **Beat** and under that, there is **Block**. If you will select any individual Block or beat the Heat Map is automatically drilled to that location.
<table><tr><td>
<img src="https://user-images.githubusercontent.com/60775543/226554951-ee8da648-90fa-4e80-b6d7-544757b6217e.png">
</td></tr></table>

 There is another page in this report **Block Level Summary**. On this page, we are providing all information about incidents on the block level.
<table><tr><td>
<img src="https://user-images.githubusercontent.com/60775543/226712407-ad4ea407-fd7a-4024-b81a-62be92e346d9.png">
</td></tr></table>

So Now Let’s create this report.
1.	**Click** on My Workspcae button in bottom left.
2.	**Click** on more options ***(three dots(…))***
<table><tr><td>
<img src="https://user-images.githubusercontent.com/60775543/226575655-71630d40-2a6e-460e-8ade-377daaa2aa11.png">
</td></tr></table>

3.	**Click** on Create Report.
<table><tr><td>
<img src="https://user-images.githubusercontent.com/60775543/226576094-c0eee24d-07d6-45fc-9dcf-5fac8aecdfb5.png">
</td></tr></table>

1. A new page will load to create reports.
     
     1. ***Click*** on **>>**  icon in the Filters pane to hide it; this will give us more space to create reports 
         for better visual experience.
     2. Expand the Table **CrimeWeatherMerged** on the right of the screen to see all fields.
<table><tr><td>
<img src="https://user-images.githubusercontent.com/60775543/226576812-d6102b47-7d75-46a0-b8be-8ee1d61ad87f.png">
</td></tr></table>

## Section B - Report / Visual Creation

Data is loaded we are going to create a report.
    
1. First we'll add a title to the report.

    1. ***Click*** on Text box ( third option in Menu Bar)
        
    2. **Add** a title by typing into the text box **Summary of incidents**
   <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226577895-573e0d15-80bb-4d33-9027-2146b77b4f5a.png"></td></tr></table>

    3. **Select** Title.
    4. **Format** Title and increase size of it and make the font Bold.
   <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226580185-f9d44a85-108b-4fa8-a08a-13061ba64078.png"></td></tr></table>

    5. **Adjust** the position of text box, so ***Click*** and ***Hold*** three dots and move the visuals or resize visual at appropriate position.
   <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226579997-42f16af7-2535-4292-be93-cf93152d30cb.png"></td></tr></table>

    6. **Click** Outside of text box to deselect visual.***(Note :*** We will follow step 6 everytime before creating a new visual otherwise after selecting a visual, previous selected visual will converted to new selected visual type and may give error).
   

We have successfully added a title to the report.

2. Our Next task is to create a Year Filter.
   
   1. **Select** Slicer from Visualization Pane.
   2. **Drag** Year Column into Field Section.
   <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226583797-8d0e0ee5-1a2a-4bf9-868a-3bad621d63a4.png"></td></tr></table>

   3. **Click** on **Format Your Visual**
   4. **Click** Slicer Settings.
   5. **Select** Style as **Tile** from dropdown under Options.
   6. Resize and rearrange the Year Filter.
   <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226585381-837dbcfe-8b22-426d-acc9-30dadf8ccdb8.png"></td></tr></table>
   

3. We have successfully created Year Slicer as a filter. Our Next step is to create a KPI to show the count of incidents.

   1. **Select** Card from Visualization Pane.
   2. **Drag** ID column into Field Section
   <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226715020-592feccd-6d8d-4006-af20-cb5e49807813.png"></td></tr></table>

   3. **Click** on Dropdown Of First Id in Field Section.
   4. **Select** Count instead of First.
   5. **Rearrange** this KPI at proper place on the page.
   <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226715223-45f133ce-8de8-4d06-a858-d7e7f371eae3.png"></td></tr></table>

4. Create a treemap to show number of Arrest.
   
   1. **Select** Treemap from Visualization Pane.
   2. **Drag** Arrest Column into category Section.
   3. **Drag** Id column into Values Section
   <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226715410-d8c9ad89-6473-4429-bd29-74a5db43118d.png"></td></tr></table>

   4. **Click** Format Your Visual.
   5. **Turn On** Data Labels
   <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226716642-cfe231b7-4e98-4ff8-a2ad-12706296a857.png"></td></tr></table>

5. Since we are creating a summary of incidents, we can rename this page to Summary.
   
    1. **Double Click** on Page name(**Page1**) to rename this.
    2. **Rename** Page1 to **Summary**
   <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226718202-3824c544-b2ff-479c-8ca6-0af45579fb9f.png"></td></tr></table>

6. we are going to show the number of incidents by type.

    1. **Select** Stacked Bar Chart from Visualization Pane.
    2. **Drag** Primary Type into Y-Axis.
    3. **Drag** Id into X-Axis
   <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226715450-a1f45dd3-c076-4704-97d5-19124f890964.png"></td></tr></table>

    4. **Click** Format Your Visual.
    5. **Turn On** Data Labels
   <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226715460-ae02bf62-81b9-472b-9996-b5cff0ec6d60.png"></td></tr></table>

7. Let's create a visual to show Count of Incidents in a map visual.

    1. **Select** Map from Visualization Pane.
    2. **Drag** Latitude Column into Latitude Field Section.
    3. **Drag** Longitude Column into Longitude Field Section
    4. **Drag** Id Column into Bubble Size Section
   <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226715490-27c98e0a-81b0-441a-9245-e2a664f4819d.png"></td></tr></table>

    5. **Click** Format Your Visual.
    6. **Turn On** Heat Map option at bottom.
   <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226715502-adb8e403-c2bc-4fb5-ad74-73f4a7f5f90c.png"></td></tr></table>

8. Lets create a new visual which will show a count of incident according to date.

    1. **Select** Line chart visual option from the Visualization Pane.
    2. **Drag** Date Column into X-Axis.
    3. **Drag** Id Column into Y-Axis
   <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226721113-ca421480-d61c-407b-b4d7-ce0549956d55.png"></td></tr></table>

    4. **Click** on DropDown of Date Hierarchy in X-Axis.
    5. **Select** Date instead of Date Hierarchy.
   <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226721125-83e7d9d7-e6fc-4aa9-878b-fb8773fdd214.png"></td></tr></table>

    **Note :**  In this visual you will see there is sudden spike that shows there could be some anomaly into data. Lets investigate that.

    6. **Click** on Add Further Analysis To Your Visual.
    7. Scroll down and **Turn On** Find Anomalies
   <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226723066-e7ca1703-7c61-4ea2-ac35-2c730f66d5d8.png"></td><td><img src="https://user-images.githubusercontent.com/60775543/226723059-2f54261a-cce9-4d30-be2e-2b7e848976ea.png"></td></tr></table>

    **Note:** You will see some highlighted points into the visual; those are potential anomalies. **Click** those points and see additional information.
    After analysing the data **Click** on the Close Button In the Anomaly Pane
    <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226724064-b29d7df5-cab7-4639-b618-0a9346e1f348.png"></td></tr></table>

9. We will create a **Matrix** to analyze data hierarchically.

    1. **Select** matrix visual from the Visualization Pane.
    2. **Drag**  these fields (**Community Area, Beat, Block, Primary Type**) into the Rows Data field.
    3. **Drag**  Id Column as Count in Values Data Field.
   <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226725563-7efd1d8b-4414-4ff4-943b-414d03af22c6.png"></td></tr></table>

    
    4. **Click**  on the down arrow of First ID in the Values Field Section.
    5. **Select**  Count instead of First
   <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226725575-22e3e0d0-f120-4165-a05e-e242b93de877.png"></td></tr></table>

    **Note:** In the Matrix visual there is a **+** button that is visible under Community Area; Expand that Community Area, under that Beat is there and by expanding Beat there is  Block.<br>
    We will add sparklines into matrix visual to understand data in better way.
    <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226726910-83e4114f-43a6-4b54-8315-5a64eb0c3052.png"></td></tr></table>

    6. **Click** on Dropdown of Count of Id.
    7. **Select** Add a sparkline, a new pop up will open.
   <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226727441-8230fda8-1730-4ed6-80c3-3dd0def02018.png"></td></tr></table>

    8. **Select** ID in Y –Axis
    9. **Select** Count in Summarization
    10. **Click** on X-Axis Dropdown,  **Expand** table and **Select** Date from list of fields.
    11. **Click** on Create.<br><br>
    **Note:** Wait for load the spikes as visual into matrix.
    <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226728160-68053b8b-785d-4d3a-a96a-f1982ffd9926.png"></td></tr></table>

    12. **Click** on Format your Visual.
    13. **Scroll** to the bottom and **expand** the Sparklines option.
    14. Under Sparkline scroll down and **expand** the Marker Option.
    15. **Select** Highest in that marker option.
   <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226729134-28a313d6-b88e-4563-a5fa-a0022a18b7d9.png"></td><td><img src="https://user-images.githubusercontent.com/60775543/226729140-cc2010a4-96a8-4c17-9546-f827165c3a09.png"></td></tr></table>

10.  We have successfully created Summary Report. Our next task is to create Block Level Details report.

    1. Click on **+** icon beside of Summary Page at bottom left side of the page.
    2. **Double click** on page name (Page1) to rename it and **type** Block Level Details as the new page name
    <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226730206-ea2ff4f8-32bc-4b99-972a-881f4d1198a4.png"></td><td><img src="https://user-images.githubusercontent.com/60775543/226730210-2395b889-d3ce-43bd-ab25-8337a2224ad7.png"></td></tr></table>

11. Create a filter on the basis of Arrest.
    
    1.  **Select** Slicer from the Visualization Pane.
    2.  **Drag** Arrest column into the Field section.<br><table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226732716-f582f559-d8d2-4561-82fe-9349e25d0c5c.png"></td></tr></table>

11. Let’s Create a date filter. 

    1. **Select** Slicer from Visualization Pane.
    2. **Drag** Date Column into the field section.
    <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226732722-62c71490-40f0-4b5e-96ad-ff4e9bab6fd8.png"></td></tr></table>

12. Now create a table to show block level details.

    1. **Select** the Table option from the visualization pane.
    2. **Add** all required fields(**Case Number,Primary Type, Description, Location Description, Arrest, Domestic**) into that by dragging fields into the column option.
    3. **Resize** the table visual
    <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226732726-1f454753-fab4-4812-be86-0728625bc0ee.png"></td></tr></table>

13. Create a Card to show the Block Name.

    1. **Select** Card visual from the Visualization Pane.
    2. **Drag** the Block Field into the Fields section.
    3. Also **drag** the Block field into the Drill Through Option.<br>
    **Note:**  We are adding **Step 3** because we will select a block from the Summary page and we'll see the information about that block in this page by drilling into the Matrix visual.
    <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226732732-a85bb53a-c143-48d8-b3cd-fe844fc1f11f.png"></td></tr></table>

14. We will add a bar chart visual to show Count of Incidents By Type.

    1. **Select** the Stacked Column Chart visual from the Visualization Pane.
    2. **Drag** the Primary Type into X-Axis.
    3. **Drag** the Id in Y-Axis
    <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226733920-ebd74ee3-760c-4c1d-b2e9-301b41aac459.png"></td></tr></table>

    4. **Click** on Format Your Visual
    5. **Turn On** Data Labels.
    6. **Resize** the visual and arrange it in a way that looks good
    <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226716642-cfe231b7-4e98-4ff8-a2ad-12706296a857.png"></td></tr></table>

15. Let’s Create a KPI to show the count of incidents.

    1. **Select** Card from the Visualization Pane
    2. **Drag** the ID column into Fields, again it will show first ID
    <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226734602-80a003e8-80f4-44c6-9325-c92664456717.png"></td></tr></table>

    3. **Click** on the dropdown of ID in the Field Section.
    4. **Select** Count from the list, it will show Count of incidents instead of First ID.
    <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226734611-4775b870-fc9f-4b7a-affa-464269fae407.png"></td></tr></table>

16. We will add one more visual to see count of incident by date.

    1. **Select** the Line Chart option from the Visualization Pane.
    2.  **Drag** Date into the X-Axis.
    3.  **Drag** ID into the Y-Axis
    <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226735755-9a98538e-88fa-427d-85f3-87b458bda9d6.png"></td></tr></table>

    4.  **Click** on the DropDown of Date in the Field Section
    5.  **Select** Date instead of Date Hierarchy.
    <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226735758-6d28c0dd-ea24-49c2-8618-fb689f40b290.png"></td></tr></table>

17. Now Save this Report.

    1. **Click** on File.
    2. **Click** Save.
    <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226735761-da44f204-73cb-4e0a-b143-cff284c79bf0.png"></td></tr></table>

    3. **Add** the report name as Crime Summary.
    4. **Click** on Save
    <table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226735763-022091ac-5335-436d-a175-a4f4a7554f2f.png"></td></tr></table>

We have completed our report creation.
This is a reference page that shows what we have created.

# Summary
<table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226736406-799bcb10-6fcb-412d-ad4c-b9dea46a8e03.png"></td></tr></table>

# Block Level Details
<table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226736416-f7657db5-c2b2-45e4-8b47-685c731bf4b8.png"></td></tr></table>

## Let's analyse the report.

1. **Go** to the Summary Page
2. **Analyse** incident by Location, Count, Area, Date.
3. **Expand** the Matrix Visual by clicking on the **+** sign.
4. Right **Click** on any block, **Select** the Drill Through option, and under that **Click** on Block level details.
5. It will redirect to the Block Level Details page with only details of that block which you have drilled into as a parameter.
<table><tr><td><img src="https://user-images.githubusercontent.com/60775543/226736820-260a65c0-9baf-4806-9f51-0e89bd6d1b8c.png"></td></tr></table>


We have completed all steps for Lab2.







    






