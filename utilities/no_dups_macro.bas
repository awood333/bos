REM  *****  BASIC  *****
Sub HighlightMatchesAndDuplicates()
    Dim oDoc As Object, oSheet As Object, oSel As Object
    Dim leftCol As Integer, rightCol As Integer
    Dim i As Integer, j As Integer
    Dim firstDataRow As Integer : firstDataRow = 4
    Dim lastDataRow As Integer : lastDataRow = 69
    Dim oCell As Object
    Dim rightValues(), rightCount(), valueCount As Integer
    Dim found As Boolean

    oDoc = ThisComponent
    oSheet = oDoc.CurrentController.ActiveSheet
    oSel = oDoc.CurrentSelection

    ' Assume user selects a cell in the right column
    rightCol = oSel.CellAddress.Column
    leftCol = rightCol - 1

    ' Gather all values from right column
    valueCount = 0
    ReDim rightValues(0)
    ReDim rightCount(0)
    For i = firstDataRow To lastDataRow
        oCell = oSheet.getCellByPosition(rightCol, i)
        found = False
        For j = 0 To valueCount - 1
            If oCell.Value = rightValues(j) Then
                rightCount(j) = rightCount(j) + 1
                found = True
                Exit For
            End If
        Next j
        If Not found And oCell.Value <> "" Then
            If valueCount > 0 Then
                ReDim Preserve rightValues(valueCount)
                ReDim Preserve rightCount(valueCount)
            End If
            rightValues(valueCount) = oCell.Value
            rightCount(valueCount) = 1
            valueCount = valueCount + 1
        End If
    Next i

    ' 1. Highlight left col cells if value is in right col
    For i = firstDataRow To lastDataRow
        oCell = oSheet.getCellByPosition(leftCol, i)
        found = False
        For j = 0 To valueCount - 1
            If oCell.Value = rightValues(j) And oCell.Value <> "" Then
                found = True
                Exit For
            End If
        Next j
        If found Then
            oCell.CellStyle = "Accent 6"
        End If
    Next i

    ' 2. Highlight duplicates in right col
    For i = firstDataRow To lastDataRow
        oCell = oSheet.getCellByPosition(rightCol, i)
        For j = 0 To valueCount - 1
            If oCell.Value = rightValues(j) And rightCount(j) > 1 And oCell.Value <> "" Then
                oCell.CellStyle = "Accent 2"
            End If
        Next j
    Next i

    MsgBox "Highlighting complete."
End Sub