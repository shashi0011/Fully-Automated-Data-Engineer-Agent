"""
DataForge AI - XLSX File Processor
Handles Excel file upload, parsing, and processing with multi-sheet support
"""

import os
import json
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from io import BytesIO


class XLSXProcessor:
    """Process Excel files (.xlsx, .xls) with multi-sheet support"""
    
    def __init__(self):
        self.base_path = "/home/z/my-project"
        self.max_file_size_mb = 50  # Max file size in MB
        self.chunk_size = 10000     # Rows per chunk for large files
    
    def validate_file(self, file_path: str) -> Dict[str, Any]:
        """
        Validate Excel file before processing.
        
        Args:
            file_path: Path to the Excel file
            
        Returns:
            Validation result with file info
        """
        if not os.path.exists(file_path):
            return {"valid": False, "error": "File not found"}
        
        # Check file extension
        ext = os.path.splitext(file_path)[1].lower()
        if ext not in ['.xlsx', '.xls', '.xlsm']:
            return {"valid": False, "error": f"Unsupported file format: {ext}. Use .xlsx, .xls, or .xlsm"}
        
        # Check file size
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        if file_size_mb > self.max_file_size_mb:
            return {
                "valid": False, 
                "error": f"File too large: {file_size_mb:.1f}MB. Maximum allowed: {self.max_file_size_mb}MB"
            }
        
        return {
            "valid": True,
            "file_path": file_path,
            "file_size_mb": round(file_size_mb, 2),
            "extension": ext
        }
    
    def get_sheet_names(self, file_path: str) -> List[str]:
        """
        Get all sheet names from an Excel file.
        
        Args:
            file_path: Path to the Excel file
            
        Returns:
            List of sheet names
        """
        try:
            xl = pd.ExcelFile(file_path)
            return xl.sheet_names
        except Exception as e:
            print(f"Error reading sheet names: {e}")
            return []
    
    def get_sheet_preview(self, file_path: str, sheet_name: str = None, rows: int = 5) -> Dict[str, Any]:
        """
        Get a preview of a sheet's data.
        
        Args:
            file_path: Path to the Excel file
            sheet_name: Name of the sheet (first sheet if None)
            rows: Number of rows to preview
            
        Returns:
            Preview data with column info
        """
        try:
            xl = pd.ExcelFile(file_path)
            
            if sheet_name is None:
                sheet_name = xl.sheet_names[0]
            
            if sheet_name not in xl.sheet_names:
                return {"error": f"Sheet '{sheet_name}' not found"}
            
            df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=rows)
            
            return {
                "sheet_name": sheet_name,
                "columns": list(df.columns),
                "column_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
                "preview_data": df.to_dict('records'),
                "total_rows_previewed": len(df)
            }
        except Exception as e:
            return {"error": str(e)}
    
    def process_sheet(
        self, 
        file_path: str, 
        sheet_name: str = None,
        skip_rows: int = 0,
        use_columns: List[str] = None
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Process a single sheet from an Excel file.
        
        Args:
            file_path: Path to the Excel file
            sheet_name: Name of the sheet (first sheet if None)
            skip_rows: Number of rows to skip from top
            use_columns: List of columns to use (all if None)
            
        Returns:
            Tuple of (DataFrame, metadata)
        """
        metadata = {
            "file_path": file_path,
            "sheet_name": sheet_name,
            "processed_at": datetime.now().isoformat(),
            "original_columns": [],
            "final_columns": [],
            "total_rows": 0,
            "columns_dropped": []
        }
        
        try:
            # Get sheet name if not specified
            if sheet_name is None:
                xl = pd.ExcelFile(file_path)
                sheet_name = xl.sheet_names[0]
                metadata["sheet_name"] = sheet_name
            
            # Read the sheet
            df = pd.read_excel(
                file_path, 
                sheet_name=sheet_name,
                skiprows=skip_rows
            )
            
            metadata["original_columns"] = list(df.columns)
            metadata["total_rows"] = len(df)
            
            # Select specific columns if requested
            if use_columns:
                available_cols = [c for c in use_columns if c in df.columns]
                missing_cols = [c for c in use_columns if c not in df.columns]
                
                if available_cols:
                    df = df[available_cols]
                metadata["columns_dropped"] = missing_cols
            
            metadata["final_columns"] = list(df.columns)
            
            # Clean column names
            df.columns = [self._clean_column_name(col) for col in df.columns]
            
            return df, metadata
            
        except Exception as e:
            metadata["error"] = str(e)
            return pd.DataFrame(), metadata
    
    def process_all_sheets(self, file_path: str) -> Dict[str, Any]:
        """
        Process all sheets from an Excel file.
        
        Args:
            file_path: Path to the Excel file
            
        Returns:
            Dictionary with all sheet data and metadata
        """
        result = {
            "file_path": file_path,
            "processed_at": datetime.now().isoformat(),
            "sheets": {},
            "summary": {
                "total_sheets": 0,
                "total_rows": 0,
                "total_columns": 0
            }
        }
        
        try:
            sheet_names = self.get_sheet_names(file_path)
            result["summary"]["total_sheets"] = len(sheet_names)
            
            for sheet_name in sheet_names:
                df, metadata = self.process_sheet(file_path, sheet_name)
                
                result["sheets"][sheet_name] = {
                    "metadata": metadata,
                    "columns": list(df.columns),
                    "row_count": len(df),
                    "column_types": {col: str(dtype) for col, dtype in df.dtypes.items()}
                }
                
                result["summary"]["total_rows"] += len(df)
                result["summary"]["total_columns"] += len(df.columns)
                
                # Store DataFrame for later use
                result["sheets"][sheet_name]["dataframe"] = df
            
            return result
            
        except Exception as e:
            result["error"] = str(e)
            return result
    
    def to_csv(
        self, 
        file_path: str, 
        output_dir: str = None,
        sheet_name: str = None,
        combine_sheets: bool = False
    ) -> Dict[str, Any]:
        """
        Convert Excel file to CSV.
        
        Args:
            file_path: Path to the Excel file
            output_dir: Directory for output files (default: data/raw)
            sheet_name: Specific sheet to convert (all sheets if None)
            combine_sheets: Combine all sheets into one CSV
            
        Returns:
            Dictionary with output file paths
        """
        if output_dir is None:
            output_dir = os.path.join(self.base_path, "data/raw")
        
        os.makedirs(output_dir, exist_ok=True)
        
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        result = {
            "source_file": file_path,
            "output_files": [],
            "processed_at": datetime.now().isoformat()
        }
        
        try:
            if sheet_name:
                # Process single sheet
                df, metadata = self.process_sheet(file_path, sheet_name)
                output_path = os.path.join(output_dir, f"{base_name}_{sheet_name}.csv")
                df.to_csv(output_path, index=False)
                result["output_files"].append({
                    "sheet": sheet_name,
                    "path": output_path,
                    "rows": len(df),
                    "columns": list(df.columns)
                })
            else:
                # Process all sheets
                sheet_names = self.get_sheet_names(file_path)
                
                if combine_sheets and len(sheet_names) > 1:
                    # Combine all sheets
                    all_dfs = []
                    for sname in sheet_names:
                        df, _ = self.process_sheet(file_path, sname)
                        df['_source_sheet'] = sname
                        all_dfs.append(df)
                    
                    combined_df = pd.concat(all_dfs, ignore_index=True)
                    output_path = os.path.join(output_dir, f"{base_name}_combined.csv")
                    combined_df.to_csv(output_path, index=False)
                    result["output_files"].append({
                        "sheet": "combined",
                        "path": output_path,
                        "rows": len(combined_df),
                        "columns": list(combined_df.columns)
                    })
                else:
                    # Save each sheet separately
                    for sname in sheet_names:
                        df, metadata = self.process_sheet(file_path, sname)
                        output_path = os.path.join(output_dir, f"{base_name}_{sname}.csv")
                        df.to_csv(output_path, index=False)
                        result["output_files"].append({
                            "sheet": sname,
                            "path": output_path,
                            "rows": len(df),
                            "columns": list(df.columns)
                        })
            
            return result
            
        except Exception as e:
            result["error"] = str(e)
            return result
    
    def to_json(
        self, 
        file_path: str, 
        output_dir: str = None,
        sheet_name: str = None,
        orient: str = "records"
    ) -> Dict[str, Any]:
        """
        Convert Excel file to JSON.
        
        Args:
            file_path: Path to the Excel file
            output_dir: Directory for output files
            sheet_name: Specific sheet to convert
            orient: JSON orientation ('records', 'index', 'columns')
            
        Returns:
            Dictionary with output file paths
        """
        if output_dir is None:
            output_dir = os.path.join(self.base_path, "data/raw")
        
        os.makedirs(output_dir, exist_ok=True)
        
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        result = {
            "source_file": file_path,
            "output_files": [],
            "processed_at": datetime.now().isoformat()
        }
        
        try:
            if sheet_name:
                df, metadata = self.process_sheet(file_path, sheet_name)
                output_path = os.path.join(output_dir, f"{base_name}_{sheet_name}.json")
                df.to_json(output_path, orient=orient, indent=2)
                result["output_files"].append({
                    "sheet": sheet_name,
                    "path": output_path,
                    "rows": len(df)
                })
            else:
                sheet_names = self.get_sheet_names(file_path)
                for sname in sheet_names:
                    df, _ = self.process_sheet(file_path, sname)
                    output_path = os.path.join(output_dir, f"{base_name}_{sname}.json")
                    df.to_json(output_path, orient=orient, indent=2)
                    result["output_files"].append({
                        "sheet": sname,
                        "path": output_path,
                        "rows": len(df)
                    })
            
            return result
            
        except Exception as e:
            result["error"] = str(e)
            return result
    
    def get_schema_info(self, file_path: str, sheet_name: str = None) -> Dict[str, Any]:
        """
        Extract schema information from an Excel file.
        
        Args:
            file_path: Path to the Excel file
            sheet_name: Specific sheet (first sheet if None)
            
        Returns:
            Schema information dictionary
        """
        try:
            df, metadata = self.process_sheet(file_path, sheet_name, rows=1000)
            
            schema = {
                "file_path": file_path,
                "sheet_name": metadata.get("sheet_name", "unknown"),
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": {}
            }
            
            for col in df.columns:
                col_data = df[col]
                
                # Determine semantic type
                semantic_type = self._detect_semantic_type(col, col_data)
                
                schema["columns"][col] = {
                    "type": str(col_data.dtype),
                    "semantic": semantic_type,
                    "nullable": bool(col_data.isnull().any()),
                    "unique_count": int(col_data.nunique()),
                    "total_count": len(col_data),
                    "null_count": int(col_data.isnull().sum()),
                    "sample_values": self._get_sample_values(col_data)
                }
            
            return schema
            
        except Exception as e:
            return {"error": str(e)}
    
    def _clean_column_name(self, name: str) -> str:
        """Clean column name for database compatibility"""
        if pd.isna(name):
            return "unnamed_column"
        
        # Convert to string and lowercase
        name = str(name).lower().strip()
        
        # Replace spaces and special characters
        name = name.replace(' ', '_').replace('-', '_')
        name = ''.join(c if c.isalnum() or c == '_' else '' for c in name)
        
        # Remove consecutive underscores
        while '__' in name:
            name = name.replace('__', '_')
        
        # Remove leading/trailing underscores
        name = name.strip('_')
        
        return name if name else "unnamed_column"
    
    def _detect_semantic_type(self, col_name: str, col_data: pd.Series) -> str:
        """Detect semantic type of a column"""
        name_lower = col_name.lower()
        
        # ID columns
        if any(kw in name_lower for kw in ['id', '_id', 'key', 'uuid']):
            return "id"
        
        # Date/time
        if any(kw in name_lower for kw in ['date', 'time', 'created', 'updated', 'dt']):
            return "datetime"
        
        # Money
        if any(kw in name_lower for kw in ['price', 'cost', 'revenue', 'amount', 'salary', 'fee']):
            return "money"
        
        # Count
        if any(kw in name_lower for kw in ['count', 'quantity', 'qty', 'num', 'number']):
            return "count"
        
        # Category
        if any(kw in name_lower for kw in ['category', 'type', 'status', 'department', 'class']):
            return "category"
        
        # Location
        if any(kw in name_lower for kw in ['city', 'state', 'country', 'region', 'location', 'address']):
            return "location"
        
        # Name
        if any(kw in name_lower for kw in ['name', 'title', 'product', 'item', 'customer', 'client']):
            return "name"
        
        # Check data patterns
        if col_data.dtype == 'object':
            sample = col_data.dropna().head(100)
            if len(sample) > 0:
                unique_ratio = sample.nunique() / len(sample)
                if unique_ratio < 0.1:
                    return "category"
        
        return "generic"
    
    def _get_sample_values(self, col_data: pd.Series, n: int = 3) -> List[Any]:
        """Get sample values from a column"""
        samples = col_data.dropna().head(n).tolist()
        # Convert to native Python types
        result = []
        for val in samples:
            if pd.isna(val):
                continue
            elif isinstance(val, (int, float, str, bool)):
                result.append(val)
            else:
                result.append(str(val))
        return result[:n]
    
    def process_upload(
        self, 
        file_content: bytes, 
        filename: str,
        output_format: str = "csv"
    ) -> Dict[str, Any]:
        """
        Process an uploaded Excel file from bytes.
        
        Args:
            file_content: Raw file bytes
            filename: Original filename
            output_format: Output format ('csv' or 'json')
            
        Returns:
            Processing result with output file paths
        """
        result = {
            "filename": filename,
            "processed_at": datetime.now().isoformat(),
            "status": "processing"
        }
        
        try:
            # Save uploaded file temporarily
            temp_dir = os.path.join(self.base_path, "data/temp")
            os.makedirs(temp_dir, exist_ok=True)
            
            temp_path = os.path.join(temp_dir, filename)
            with open(temp_path, 'wb') as f:
                f.write(file_content)
            
            # Validate
            validation = self.validate_file(temp_path)
            if not validation["valid"]:
                result["status"] = "error"
                result["error"] = validation["error"]
                os.remove(temp_path)
                return result
            
            # Get sheet info
            sheet_names = self.get_sheet_names(temp_path)
            result["sheets"] = sheet_names
            result["file_size_mb"] = validation["file_size_mb"]
            
            # Convert to desired format
            if output_format == "json":
                convert_result = self.to_json(temp_path)
            else:
                convert_result = self.to_csv(temp_path)
            
            if "error" in convert_result:
                result["status"] = "error"
                result["error"] = convert_result["error"]
            else:
                result["status"] = "success"
                result["output_files"] = convert_result["output_files"]
                
                # Get schema for first sheet
                if sheet_names:
                    result["schema"] = self.get_schema_info(temp_path, sheet_names[0])
            
            # Clean up temp file
            os.remove(temp_path)
            
            return result
            
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            return result
