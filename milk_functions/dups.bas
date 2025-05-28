Sub duplicates()
    Dim oSheet As Object
    Dim oCell As Object
    Dim oRange As Object, oLeftRange As Object
    Dim oCursor As Object
    Dim oActiveCell As Object
    Dim nCol As Long, nLeftCol As Long
    Dim nStartRow As Long, nEndRow As Long
    Dim oData As Variant, oLeftData As Variant
    Dim i As Long, j As Long
    Dim nRows As Long
    Dim oRangeToName As Object
    Dim oRangeAddress As New com.sun.star.table.CellRangeAddress
    
    nRows = 70 ' Number of rows to check from the active cell
    
    ' Get the active sheet and cell
    oSheet = ThisComponent.CurrentController.ActiveSheet
    oActiveCell = ThisComponent.CurrentSelection

    nCol = oActiveCell.CellAddress.Column
    nLeftCol = nCol - 1

    ' Get the start and end rows (70 rows from the active cell)
    nStartRow = oActiveCell.CellAddress.Row
    nEndRow = nStartRow + nRows - 1

    ' Limit the end row if it exceeds the sheet's last row
    If nEndRow > oSheet.Rows.Count - 1 Then
        nEndRow = oSheet.Rows.Count - 1
    End If

    ' Get the data from the active column and the adjacent left column
    oRange = oSheet.getCellRangeByPosition(nCol, nStartRow, nCol, nEndRow)
    oLeftRange = oSheet.getCellRangeByPosition(nLeftCol, nStartRow, nLeftCol, nEndRow)
    oData = oRange.getDataArray()
    oLeftData = oLeftRange.getDataArray()

    ' Iterate through the column to find duplicates in the same column
    For i = 0 To UBound(oData)
        If oData(i)(0) <> "" Then ' Ignore empty cells
            For j = i + 1 To UBound(oData)
                If oData(i)(0) = oData(j)(0) Then
                    ' Highlight the duplicates in the same column with yellow
                    oSheet.getCellByPosition(nCol, nStartRow + j).CellBackColor = RGB(255, 255, 0) ' Yellow
                    oSheet.getCellByPosition(nCol, nStartRow + i).CellBackColor = RGB(255, 255, 0) ' Yellow
                End If
            Next j
        End If
    Next i

    ' Iterate through the column to find duplicates with the left adjacent column
    For i = 0 To UBound(oData)
        If oData(i)(0) <> "" Then ' Ignore empty cells
            For j = 0 To UBound(oLeftData)
                If oData(i)(0) = oLeftData(j)(0) Then
                    ' Only highlight if not already highlighted for same column duplicates
                    If oSheet.getCellByPosition(nCol, nStartRow + i).CellBackColor <> RGB(255, 255, 0) Then
                        oSheet.getCellByPosition(nCol, nStartRow + i).CellBackColor = RGB(173, 216, 230) ' Light Blue
                    End If
                    If oSheet.getCellByPosition(nLeftCol, nStartRow + j).CellBackColor <> RGB(255, 255, 0) Then
                        oSheet.getCellByPosition(nLeftCol, nStartRow + j).CellBackColor = RGB(173, 216, 230) ' Light Blue
                    End If
                End If
            Next j
        End If
    Next i

    ' Select the entire range (70 rows starting from the active cell including left column)
    oRangeToName = oSheet.getCellRangeByPosition(nLeftCol, nStartRow, nCol, nEndRow)
    ThisComponent.CurrentController.Select(oRangeToName)
    
    ' Name the module "duplicates" (you need to manually set the module name to "duplicates" in the LibreOffice Basic editor)
    ' There is no way to programmatically rename a module in LibreOffice Basic, it has to be done manually.

End Sub
