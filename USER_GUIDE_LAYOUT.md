# Pacific Canbriam Energy - Production Update System
## Complete User Guide Document Layout & Information

---

## DOCUMENT STRUCTURE OUTLINE

### 1. COVER PAGE
- Title: "Pacific Canbriam Energy - Production Update System"
- Subtitle: "User Guide"
- Version: [Current Version]
- Date: [Current Date]
- Company Logo/Header

### 2. TABLE OF CONTENTS
- Introduction
- System Overview
- Getting Started
- Main Interface Overview
- Feature Modules (7 main features)
- Settings Configuration
- Troubleshooting
- Appendices

### 3. INTRODUCTION
**Purpose of Document:**
- Comprehensive guide for end users of the Production Update System
- Step-by-step instructions for all features
- Best practices and troubleshooting tips

**Target Audience:**
- Production Engineers
- Data Analysts
- Production Accounting Staff
- System Administrators

**Document Conventions:**
- **Bold text** = Button names, menu items, important terms
- *Italic text* = File names, paths, technical terms
- `Code font` = Database table names, field names
- Screenshot references: [Figure X]
- Warning boxes: ⚠️
- Information boxes: ℹ️
- Success indicators: ✅
- Error indicators: ❌

---

## 4. SYSTEM OVERVIEW

### 4.1 What is the Production Update System?
The Production Update System is a comprehensive ETL (Extract, Transform, Load) application designed to:
- Retrieve daily production data from Snowflake/Prodview
- Import and manage well master data
- Process production accounting allocations
- Import survey and type curve data
- Update sales ratios and public data
- Maintain data integrity across SQL Server databases

### 4.2 System Architecture
- **Frontend:** PyQt5 GUI Application
- **Backend Databases:**
  - SQL Server: `Re_Main_Production` database
    - `PCE_WM` - Well Master table
    - `PCE_CDA` - Daily production data
    - `PCE_Production` - Monthly production summaries
    - `PCE_Surveys` - Survey Data 
  - Snowflake: Production data warehouse
- **Configuration:** `settings.ini` file
- **Environment:** `.env` file for credentials

### 4.3 Key Features
1. **Well Master List Management** - View, edit, add, and manage well information
2. **Prodview/Snowflake Daily Production Retrieve** - Pull and process daily production data
3. **Production Accounting Allocations (PA)** - Monthly allocation processing
4. **Public Sales Data and Ratios** - Update sales ratios and calculated fields
5. **Survey Data Import** - Import well survey information
6. **Type Curves Import** - Import type curve data
7. **Exports/Reports** - (Coming soon)

---

## 5. GETTING STARTED

### 5.1 System Requirements
- **Operating System:** Windows 10/11
- **Python:** 3.8+ (if running from source)
- **Database Access:**
  - SQL Server connection to `CALVMSQL02\Re_Main_Production`
  - Snowflake account access
- **Network:** Access to company network drives
- **Permissions:** Read/write access to SQL Server database

### 5.2 Installation
1. Locate the application executable: `ProductionUpdate.exe`
2. Ensure you have network access to:
   - SQL Server: `CALVMSQL02`
   - Snowflake data warehouse
   - Network file shares (I: drive)
3. Verify `.env` file exists with Snowflake credentials
4. Verify `settings.ini` file exists (will be created on first run)

### 5.3 First-Time Setup
1. Launch the application
2. Click **⚙️ Settings** button (top-right)
3. Configure:
   - SQL Server connection (default: `CALVMSQL02`, `Re_Main_Production`)
   - File paths for:
     - ValNav Template
     - Accumap Template
     - Survey File
     - Type Curves File
4. Click **Save Settings**
5. Verify database connection in main log area

### 5.4 Launching the Application
- Double-click `ProductionUpdate.exe`
- Main window opens with:
  - Company header: "Pacific Canbriam Energy LTD"
  - Settings button (top-right)
  - 7 main operation buttons
  - Operation log area (bottom)
  - Status bar: "Ready"

---

## 6. MAIN INTERFACE OVERVIEW

### 6.1 Main Window Layout

**Header Section:**
- **Company Name:** "Pacific Canbriam Energy LTD" (centered, green header)
- **Settings Button:** ⚙️ Settings (top-right, gray button)

**Sub-Header:**
- "Production Update System" (blue text, centered)

**Main Operation Buttons (7 buttons, vertical list):**
1. **📋 Well Master List** - Manage well information
2. **❄️ Prodview/Snowflake Daily Production Retrieve** - Pull daily production data
3. **📊 Production Accounting Allocations (PA)** - Monthly allocations
4. **📈 Public Sales Data and Ratios** - Update sales ratios
5. **📐 Survey Data Import** - Import survey data
6. **📊 Type Curves Import** - Import type curves
7. **📁 Exports / Reports** - (Coming soon)

**Operation Log Area:**
- Read-only text area
- Shows timestamped messages: `[HH:MM:SS] Message`
- Auto-scrolls to latest messages
- Monospace font (Consolas)
- Light blue background

**Status Bar:**
- Shows current status: "Ready", "Processing...", etc.
- Gray italic text

### 6.2 Button Behavior
- Buttons are **checkable** (can be selected)
- Only **one button can be selected at a time**
- Selected button turns **green with orange border**
- Clicking a button opens its corresponding dialog
- Button automatically unchecks when dialog closes

### 6.3 Navigation Tips
- All operations open in **modal dialogs** (must close before using main window)
- Use **Cancel** or **Close** buttons to exit dialogs
- Check **Operation Log** for status updates
- **Settings** can be accessed from any screen

---

## 7. FEATURE MODULES - DETAILED INSTRUCTIONS

### 7.1 📋 WELL MASTER LIST

**Purpose:** View, edit, add, and manage well master information in the `PCE_WM` table.

**Access:** Click **📋 Well Master List** button

**Dialog Features:**
- **Two Tabs:**
  1. **View/Edit Tab** - Browse and edit existing wells
  2. **Add New Tab** - Add new well records

**View/Edit Tab:**
- **Search Bar:** Filter wells by name, GasIDREC, or PressuresIDREC
- **Table Columns (18 columns):**
  1. Checkbox (for selection)
  2. Well Name
  3. GasIDREC
  4. PressuresIDREC
  5. Formation Producer
  6. Layer Producer
  7. Fault Block
  8. Pad Name
  9. Completions
  10. Lateral Length
  11. Horizontal Distance Right
  12. Horizontal Distance Left
  13. Vertical Distance Above
  14. Vertical Distance Below
  15. Value Nav UWI
  16. Orient
  17. Composite Name
  18. Exception

- **Editable Fields:** Click any cell to edit (except Well Name, GasIDREC, PressuresIDREC)
- **Dropdown Fields:** Formation, Layer, Fault Block, Pad Name, Orient, Exception
- **Buttons:**
  - **Refresh** - Reload data from database
  - **Save Changes** - Save all edits to database
  - **Export to Excel** - Export current view to Excel
  - **Import from Excel** - Import well data from Excel file
  - **Update from Snowflake** - Sync well data from Snowflake
  - **Remove Selected** - Delete selected wells (with confirmation)

**Add New Tab:**
- **Staged Wells Table:** Shows wells ready to be added
- **Add Row Button:** Add new empty row to staging table
- **Save to Database Button:** Commit all staged wells
- **Info Label:** Shows count of staged wells

**Step-by-Step: Editing a Well**
1. Click **📋 Well Master List**
2. In View/Edit tab, use search bar to find well
3. Click cell you want to edit
4. For dropdown fields, select from dropdown
5. For text fields, type new value
6. Click **Save Changes** button
7. Confirm success message

**Step-by-Step: Adding a New Well**
1. Click **📋 Well Master List**
2. Click **Add New** tab
3. Click **Add Row** button
4. Fill in required fields:
   - Well Name (required)
   - GasIDREC (required)
   - PressuresIDREC (optional)
5. Fill in optional fields as needed
6. Click **Save to Database** button
7. Confirm success message

**Step-by-Step: Importing from Excel**
1. Click **📋 Well Master List**
2. Click **Import from Excel** button
3. Select Excel file
4. Map columns (if needed)
5. Review import preview
6. Click **Import** button
7. Verify imported wells in table

**Important Notes:**
- ⚠️ **Well Name, GasIDREC, PressuresIDREC** cannot be edited (primary keys)
- ⚠️ **Exception** field: Set to "Y" or "N" to exclude/include from production processing
- ✅ Changes are saved immediately when clicking **Save Changes**
- ✅ Use **Refresh** to reload if changes were made externally

---

### 7.2 ❄️ PRODVIEW/SNOWFLAKE DAILY PRODUCTION RETRIEVE

**Purpose:** Retrieve daily production data from Snowflake and update `PCE_CDA` and `PCE_Production` tables.

**Access:** Click **❄️ Prodview/Snowflake Daily Production Retrieve** button

**Dialog Features:**

**Update Range Section:**
- **From:** Dropdown (months, going back 36 months)
- **To:** Dropdown (months, default: current month)

**Update Mode Section (Radio Buttons):**
1. **Full Rebuild Mode** (default)
   - Processes ALL historical data
   - Clears and rebuilds entire `PCE_Production` table
   - Takes 30-40 minutes (full rebuild)
   - Use for: Initial setup, major corrections, annual rebuilds

2. **Quick Update Mode**
   - Processes only selected month range
   - Updates `PCE_CDA` for selected months
   - Updates `PCE_Production` for selected months
   - Recalculates sequences for affected wells only
   - Updates cumulatives incrementally
   - Use for: Monthly updates, recent corrections

**Info Section:**
- Shows what the operation will do based on selected mode
- Updates dynamically when mode changes

**Progress Bar:**
- Shows overall progress (0-100%)
- Appears during processing
- Blue progress indicator

**Status Label:**
- Shows current status: "Ready to start", "Processing...", "Complete"

**Results Area:**
- Scrollable text area
- Shows detailed progress messages
- Timestamped log entries
- Shows:
  - Connection status
  - Data retrieval progress
  - Row counts for each data source
  - Insert progress
  - Completion summary

**Buttons:**
- **Run Update** - Start the process
- **Cancel** - Close dialog (cancels if running)

**Step-by-Step: Quick Update (Recommended for Monthly Use)**
1. Click **❄️ Prodview/Snowflake Daily Production Retrieve**
2. Select **Quick Update Mode** radio button
3. Select **From** month (e.g., "Jan 2024")
4. Select **To** month (e.g., "Mar 2024")
5. Review info section to confirm what will happen
6. Click **Run Update** button
7. Confirm warning dialog (if shown)
8. Monitor progress bar and results area
9. Wait for completion message
10. Review summary in results area
11. Click **Cancel** or **Close** to exit

**Step-by-Step: Full Rebuild (Use Sparingly)**
1. Click **❄️ Prodview/Snowflake Daily Production Retrieve**
2. Select **Full Rebuild Mode** radio button
3. ⚠️ **WARNING:** This will process ALL historical data
4. Click **Run Update** button
5. Confirm warning dialog
6. ⚠️ **Estimated time: 30-40 minutes**
7. Monitor progress bar and results area
8. Do not close dialog during processing
9. Wait for completion message
10. Review summary in results area

**Data Sources Retrieved:**
- ECF (Effluent Factor) data
- Gas Wellhead production data
- CGR (Condensate Gas Ratio) data
- WGR (Water Gas Ratio) data
- Pressure data (Tubing, Casing, Choke)
- Allocation data (Gathered Gas, Condensate, NGL)
- Water allocation data

**What Gets Updated:**
- `PCE_CDA` table: Daily production records
- `PCE_Production` table: Monthly summaries with sequences and cumulatives

**Important Notes:**
- ⚠️ **Full Rebuild** takes 30-40 minutes - plan accordingly
- ✅ **Quick Update** is recommended for regular monthly updates
- ✅ Progress is logged in real-time in results area
- ✅ Can cancel during processing (not recommended)
- ✅ Database connection is validated before starting

---

### 7.3 📊 PRODUCTION ACCOUNTING ALLOCATIONS (PA)

**Purpose:** Process monthly production accounting allocations from ValNav and Accumap files.

**Access:** Click **📊 Production Accounting Allocations (PA)** button

**Dialog Features:**

**Select Month Section:**
- **Month:** Dropdown (months, default: current month)

**ValNav File Section:**
- **Path:** Shows path from Settings (read-only)
- Status indicator: ✅ Valid / ❌ Not found

**Public Data Accumap File Section:**
- **Path:** Shows path from Settings (read-only)
- Status indicator: ✅ Valid / ❌ Not found

**Status Section:**
- **Database Status:** ✅ Connected / ❌ Not connected
- **ValNav File Status:** ✅ Found / ❌ Not found
- **Accumap File Status:** ✅ Found / ❌ Not found

**Progress Bar:**
- Shows processing progress (0-100%)
- Appears during processing

**Results Area:**
- Scrollable text area
- Shows:
  - Month being processed
  - File validation status
  - Processing steps
  - Records processed
  - Wells matched
  - Completion summary
  - Duration

**Buttons:**
- **Run Loader** - Start the process
- **Close** - Close dialog

**Step-by-Step: Processing Monthly Allocations**
1. Click **📊 Production Accounting Allocations (PA)**
2. Verify all status indicators show ✅ (green checkmarks)
3. If any show ❌:
   - Check Settings for correct file paths
   - Ensure files exist at specified paths
   - Ensure database connection is available
4. Select month from dropdown
5. Click **Run Loader** button
6. Confirm dialog showing month to process
7. Monitor progress bar and results area
8. Wait for completion message
9. Review summary:
   - ValNav records processed
   - Accumap records processed
   - Wells successfully matched
   - Wells added with zeros
   - Total wells processed
   - Duration
10. Click **Close** to exit

**File Requirements:**
- **ValNav Template:** Excel file with production accounting data
- **Accumap Template:** Excel file with public sales data
- Both files must be configured in Settings before use

**Important Notes:**
- ⚠️ **File paths must be configured in Settings first**
- ✅ Status indicators show real-time validation
- ✅ Process one month at a time
- ✅ Results show detailed matching statistics
- ⚠️ **Ensure ValNav and Accumap files are up-to-date for selected month**

---

### 7.4 📈 PUBLIC SALES DATA AND RATIOS

**Purpose:** Update calculated fields in `PCE_CDA` based on sales ratios and public data.

**Access:** Click **📈 Public Sales Data and Ratios** button

**Dialog Features:**

**Select Month Range Section:**
- **From:** Dropdown (months)
- **To:** Dropdown (months, default: current month)

**Info Section:**
- Shows what fields will be updated:
  - Gas - S2 Production
  - Gas - Sales Production
  - Condensate - Sales Production
  - Sales ratios and calculated fields

**Progress Bar:**
- Shows processing progress (0-100%)
- Appears during processing

**Results Area:**
- Scrollable text area
- Shows:
  - Month range being processed
  - Processing steps
  - Records updated
  - Completion summary

**Buttons:**
- **Run Update** - Start the process
- **Close** - Close dialog

**Step-by-Step: Updating Sales Ratios**
1. Click **📈 Public Sales Data and Ratios**
2. Select **From** month
3. Select **To** month
4. Review info section
5. Click **Run Update** button
6. Confirm dialog
7. Monitor progress bar and results area
8. Wait for completion message
9. Review summary
10. Click **Close** to exit

**What Gets Updated:**
- Calculated fields in `PCE_CDA` table
- Sales production values
- Sales ratios

**Important Notes:**
- ✅ Can process multiple months at once
- ✅ Updates are incremental (only selected range)
- ✅ Progress is logged in real-time

---

### 7.5 📐 SURVEY DATA IMPORT

**Purpose:** Import well survey data from Excel files into the database.

**Access:** Click **📐 Survey Data Import** button

**Dialog Features:**

**File Selection Section:**
- **Survey File Path:** Shows path from Settings (read-only)
- **Browse Button:** Select different file (optional)
- Status indicator: ✅ Valid / ❌ Not found

**Import Mode Section (Radio Buttons):**
1. **Append Mode** (default)
   - Adds new survey records
   - Does not delete existing data
   - Use for: Adding new surveys, incremental updates

2. **Overwrite Mode**
   - Deletes existing survey data first
   - Then imports all data from file
   - Use for: Complete refresh, corrections

**Status Section:**
- **File Status:** ✅ Found / ❌ Not found
- **Database Status:** ✅ Connected / ❌ Not connected

**Progress Bar:**
- Shows import progress (0-100%)
- Appears during processing

**Results Area:**
- Scrollable text area
- Shows:
  - File validation
  - Import mode
  - Records processed
  - Records inserted
  - Completion summary

**Buttons:**
- **Run Import** - Start the import
- **Close** - Close dialog

**Step-by-Step: Importing Survey Data (Append Mode)**
1. Click **📐 Survey Data Import**
2. Verify file status shows ✅
3. If file path is incorrect:
   - Click **Browse** to select correct file
   - Or update path in Settings
4. Select **Append Mode** radio button
5. Click **Run Import** button
6. Confirm dialog
7. Monitor progress bar and results area
8. Wait for completion message
9. Review summary:
   - Records read from file
   - Records inserted
   - Duration
10. Click **Close** to exit

**Step-by-Step: Importing Survey Data (Overwrite Mode)**
1. Click **📐 Survey Data Import**
2. Verify file status shows ✅
3. Select **Overwrite Mode** radio button
4. ⚠️ **WARNING:** This will delete all existing survey data
5. Click **Run Import** button
6. Confirm warning dialog
7. Monitor progress bar and results area
8. Wait for completion message
9. Review summary
10. Click **Close** to exit

**File Requirements:**
- Excel file (.xlsx or .xls)
- Must contain survey data in expected format
- File path can be configured in Settings

**Important Notes:**
- ⚠️ **Overwrite Mode deletes all existing survey data** - use with caution
- ✅ **Append Mode** is safer for regular updates
- ✅ File format must match expected structure
- ✅ Progress shows detailed import statistics

---

### 7.6 📊 TYPE CURVES IMPORT

**Purpose:** Import type curve data from Excel files directly into the `PCE_Production` table.

**Access:** Click **📊 Type Curves Import** button

**Dialog Features:**

**File Selection Section:**
- **Type Curves File Path:** Shows path from Settings (read-only)
- **Browse Button:** Select different file (optional)
- Status indicator: ✅ Valid / ❌ Not found

**Status Section:**
- **File Status:** ✅ Found / ❌ Not found
- **Database Status:** ✅ Connected / ❌ Not connected

**Progress Bar:**
- Shows import progress (0-100%)
- Appears during processing

**Results/Log Area:**
- Scrollable text area
- Shows:
  - File validation
  - Reading Excel progress
  - Database connection
  - Data processing steps
  - Records deleted (YE2% wells)
  - Records inserted
  - Completion summary

**Buttons:**
- **Run Import** - Start the import
- **Cancel** - Close dialog (cancels if running)

**Step-by-Step: Importing Type Curves**
1. Click **📊 Type Curves Import**
2. Verify file status shows ✅
3. If file path is incorrect:
   - Click **Browse** to select correct file
   - Or update path in Settings
4. Click **Run Import** button
5. ⚠️ **WARNING DIALOG APPEARS:**
   - "This will import data directly into the Production table."
   - "This operation will first DELETE ALL existing 'YE2%' wells from PCE_Production and then insert new data."
   - "Are you sure you want to continue?"
6. Click **Yes** to confirm or **No** to cancel
7. If confirmed, monitor progress bar and log area
8. Wait for completion message
9. Review summary:
   - Records deleted
   - Records inserted
   - Duration
10. Click **Cancel** or **Close** to exit

**What Gets Updated:**
- **DELETES:** All existing records in `PCE_Production` where Well Name starts with "YE2"
- **INSERTS:** New type curve data from Excel file into `PCE_Production`

**File Requirements:**
- Excel file (.xlsx or .xls)
- Must contain type curve data in expected format
- File path: `I:/ResEng/Tools/Programmers Paradise/mvp_cda_load/PCE_TCs_MTHLY.xlsx` (default)

**Important Notes:**
- ⚠️ **CRITICAL WARNING:** This operation DELETES all 'YE2%' well data from Production table
- ⚠️ **Always verify file is correct before importing**
- ⚠️ **Consider backing up Production table before running**
- ✅ Progress shows detailed steps
- ✅ Can cancel during processing (not recommended)

---

### 7.7 📁 EXPORTS / REPORTS

**Purpose:** (Feature coming soon)

**Access:** Click **📁 Exports / Reports** button

**Current Status:**
- Shows "🚧 Coming Soon 🚧" message
- Feature is under development
- Will be available in future update

**Dialog Features:**
- Title: "📁 Exports / Reports"
- Coming Soon message
- Description: "The Exports / Reports feature is currently under development. This functionality will be available in a future update."
- **Close** button

---

## 8. SETTINGS CONFIGURATION

**Access:** Click **⚙️ Settings** button (top-right of main window)

**Dialog Features:**

### 8.1 SQL Server Connection Settings

**Server:**
- Default: `CALVMSQL02`
- Enter SQL Server name or IP address

**Database:**
- Default: `Re_Main_Production`
- Enter database name

**Purpose:** Configure connection to SQL Server where production data is stored.

### 8.2 Default File Paths

**ValNav Template:**
- Path to ValNav Excel file
- Used by: Production Accounting Allocations (PA)
- Default location: `I:/ResEng/Production/PA Monthly Actuals`
- **Browse** button to select file

**Accumap Template:**
- Path to Public Data Accumap Excel file
- Used by: Production Accounting Allocations (PA)
- Default location: `I:/ResEng/Production/Prod Macros/Macro 3`
- **Browse** button to select file

**Survey File:**
- Path to Survey Excel file
- Used by: Survey Data Import
- **Browse** button to select file

**Type Curves File:**
- Path to Type Curves Excel file
- Used by: Type Curves Import
- Default location: `I:/ResEng/Tools/Programmers Paradise/mvp_cda_load/PCE_TCs_MTHLY.xlsx`
- **Browse** button to select file

### 8.3 Saving Settings

**Buttons:**
- **Save Settings** (green button) - Saves all settings to `settings.ini` file
- **Cancel** (gray button) - Closes without saving

**Step-by-Step: Configuring Settings**
1. Click **⚙️ Settings** button
2. Enter SQL Server name (if different from default)
3. Enter Database name (if different from default)
4. For each file path:
   - Click **Browse** button
   - Navigate to file location
   - Select file
   - Path appears in text field
5. Click **Save Settings** button
6. Confirm success message: "Settings have been saved successfully."
7. Settings are saved to `settings.ini` file in application directory

**Important Notes:**
- ✅ Settings are saved immediately when clicking **Save Settings**
- ✅ File paths can use network drives (e.g., I: drive)
- ✅ Settings persist between application sessions
- ⚠️ **Verify file paths are accessible** before saving
- ⚠️ **SQL Server settings must be correct** for database operations to work

---

## 9. OPERATION LOG

**Location:** Bottom section of main window

**Purpose:** 
- Shows timestamped messages from all operations
- Helps track what operations have been performed
- Useful for troubleshooting

**Features:**
- **Read-only** text area
- **Auto-scrolling** to latest messages
- **Timestamp format:** `[HH:MM:SS] Message`
- **Monospace font** (Consolas) for readability
- **Light blue background**

**Message Types:**
- Operation started: `[14:30:15] Opening Prodview/Snowflake Update dialog...`
- Settings saved: `[14:31:22] Settings saved`
- Operation completed: `[14:35:45] Operation complete`

**Tips:**
- Scroll up to see older messages
- Log persists during application session
- Log clears when application is restarted

---

## 10. TROUBLESHOOTING

### 10.1 Common Issues

**Issue: Cannot connect to database**
- **Symptoms:** Error messages about database connection
- **Solutions:**
  1. Verify SQL Server name in Settings
  2. Verify database name in Settings
  3. Check network connectivity to SQL Server
  4. Verify Windows authentication is working
  5. Contact IT if SQL Server is down

**Issue: File not found errors**
- **Symptoms:** Status shows ❌ Not found for files
- **Solutions:**
  1. Check file path in Settings
  2. Verify file exists at specified path
  3. Check network drive access (I: drive)
  4. Verify file permissions (read access)
  5. Use Browse button to reselect file

**Issue: Snowflake connection errors**
- **Symptoms:** Errors when retrieving data from Snowflake
- **Solutions:**
  1. Verify `.env` file exists in application directory
  2. Check Snowflake credentials in `.env` file
  3. Verify network connectivity to Snowflake
  4. Check if Snowflake account is accessible
  5. Contact IT if Snowflake is down

**Issue: Operation takes too long**
- **Symptoms:** Progress bar stuck, no updates
- **Solutions:**
  1. Check network connectivity
  2. Verify database is responding
  3. Check if other users are running operations
  4. For Full Rebuild: 30-40 minutes is normal
  5. For Quick Update: Should complete in 1-5 minutes per month
  6. If stuck > 10 minutes, cancel and retry

**Issue: Data not updating correctly**
- **Symptoms:** Changes not reflected in database
- **Solutions:**
  1. Verify you clicked **Save** button (not just Close)
  2. Refresh data view (click Refresh button)
  3. Check Operation Log for error messages
  4. Verify database permissions (write access)
  5. Check if another user has locks on tables

**Issue: Application crashes or freezes**
- **Symptoms:** Application becomes unresponsive
- **Solutions:**
  1. Wait 2-3 minutes (long operations may appear frozen)
  2. Check Operation Log for progress updates
  3. If truly frozen, close application (may need Task Manager)
  4. Restart application
  5. Check if operation completed before crash
  6. Contact IT support if issue persists

### 10.2 Error Messages

**"Missing required Snowflake configuration in .env file"**
- **Cause:** `.env` file missing or incomplete
- **Solution:** Ensure `.env` file exists with all required Snowflake credentials

**"Failed to connect to Snowflake"**
- **Cause:** Network or credential issue
- **Solution:** Check network, verify credentials, contact IT

**"Settings file not found"**
- **Cause:** `settings.ini` file missing
- **Solution:** Configure settings and save (will create file)

**"Database connection failed"**
- **Cause:** SQL Server unreachable or credentials invalid
- **Solution:** Check SQL Server name, network, contact IT

### 10.3 Best Practices

1. **Before Running Operations:**
   - Verify Settings are configured correctly
   - Check file paths are accessible
   - Verify database connection
   - Review what operation will do

2. **During Operations:**
   - Do not close dialogs during processing
   - Monitor progress bar and log messages
   - Be patient for long operations (Full Rebuild)

3. **After Operations:**
   - Review completion summaries
   - Check Operation Log for any warnings
   - Verify data in database if needed

4. **Regular Maintenance:**
   - Use Quick Update mode for monthly updates
   - Use Full Rebuild only when necessary
   - Keep file paths in Settings up-to-date
   - Review and clear Operation Log periodically

---

## 11. APPENDICES

### Appendix A: Database Tables Reference

**PCE_WM (Well Master)**
- Primary key: Well Name, GasIDREC, PressuresIDREC
- Contains: Well information, formations, layers, fault blocks, pad names, completions, distances, UWI, orient, composite name, exception flag

**PCE_CDA (Daily Production Data)**
- Primary key: Well Name, ProdDate
- Contains: Daily production values, ratios, pressures, allocations, gathered production

**PCE_Production (Monthly Production Summaries)**
- Primary key: Well Name, Date (monthly)
- Contains: Monthly production summaries, sequences, cumulatives, averages

### Appendix B: File Formats

**ValNav Template:**
- Excel format (.xlsx or .xls)
- Must contain production accounting allocation data
- Specific column structure required

**Accumap Template:**
- Excel format (.xlsx or .xls)
- Must contain public sales data
- Specific column structure required

**Survey File:**
- Excel format (.xlsx or .xls)
- Must contain well survey data
- Specific column structure required

**Type Curves File:**
- Excel format (.xlsx or .xls)
- Must contain type curve data
- Well names must start with "YE2"
- Specific column structure required

### Appendix C: Keyboard Shortcuts

- **None currently implemented** - All operations use mouse clicks

### Appendix D: Contact Information

**For Technical Support:**
- Contact IT Department
- Provide:
  - Error messages from Operation Log
  - Screenshots of issues
  - Steps to reproduce problem

**For Data Issues:**
- Contact Production Engineering Team
- Verify data in source systems (Snowflake, Excel files)

---

## 12. DOCUMENT METADATA

**Document Version:** 1.0
**Last Updated:** [Current Date]
**Application Version:** [Current Version]
**Author:** Production Update System Development Team
**Review Cycle:** Annual or as features are added

---

## NOTES FOR CHATGPT FORMATTING:

1. **Use professional document formatting:**
   - Clear headings and subheadings
   - Consistent numbering
   - Table of contents with page numbers
   - Page breaks between major sections

2. **Include screenshot placeholders:**
   - [Figure 1: Main Interface]
   - [Figure 2: Settings Dialog]
   - [Figure 3: Well Master List]
   - [Figure 4: Prodview Update Dialog]
   - [Figure 5: Monthly Loader Dialog]
   - [Figure 6: Survey Import Dialog]
   - [Figure 7: Type Curves Import Dialog]
   - [Figure 8: Sales Ratios Dialog]

3. **Add visual elements:**
   - Warning boxes with ⚠️ icon
   - Information boxes with ℹ️ icon
   - Success indicators with ✅
   - Error indicators with ❌
   - Code blocks for technical terms
   - Tables for structured data

4. **Formatting style:**
   - Use bold for button names and important terms
   - Use italic for file names and paths
   - Use code font for database/technical terms
   - Use bullet points for lists
   - Use numbered lists for step-by-step instructions

5. **Add cross-references:**
   - Link to related sections
   - Reference figures
   - Link to appendices

6. **Include a glossary** (optional):
   - Define technical terms
   - Acronyms (ETL, PA, CDA, etc.)

7. **Add index** (optional):
   - Alphabetical index of topics
   - Page number references
