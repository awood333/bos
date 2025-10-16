REM  *****  BASIC  *****
Sub CopyFormatsFromFormatSheet()
    Dim oDoc As Object, oSheets As Object
    Dim oFormatSheet As Object, oReportSheet As Object
    Dim oController As Object
    Dim dispatcher As Object
    Dim args()

    oDoc = ThisComponent
    oSheets = oDoc.Sheets
    oController = oDoc.CurrentController
    dispatcher = createUnoService("com.sun.star.frame.DispatchHelper")

    ' Activate and select all in "format" sheet
    oController.ActiveSheet = oSheets.getByName("format")
    Dim argsSelectRange(0) As New com.sun.star.beans.PropertyValue
    argsSelectRange(0).Name = "ToPoint"
    argsSelectRange(0).Value = "$A$1:$AC$100"
    dispatcher.executeDispatch(oController.Frame, ".uno:GoToCell", "", 0, argsSelectRange())


    ' Copy selection
    dispatcher.executeDispatch(oController.Frame, ".uno:Copy", "", 0, args())

    ' Switch to "report_milk" sheet and select A1
    oController.ActiveSheet = oSheets.getByName("report_milk")
    Dim argsGoToA1(0) As New com.sun.star.beans.PropertyValue
    argsGoToA1(0).Name = "ToPoint"
    argsGoToA1(0).Value = "$A$1"
    dispatcher.executeDispatch(oController.Frame, ".uno:GoToCell", "", 0, argsGoToA1())

    ' Paste special: formats only
    Dim argsPaste(5) As New com.sun.star.beans.PropertyValue
    argsPaste(0).Name = "Flags"
    argsPaste(0).Value = "T" ' T = formats only
    argsPaste(1).Name = "FormulaCommand"
    argsPaste(1).Value = 0
    argsPaste(2).Name = "SkipEmptyCells"
    argsPaste(2).Value = False
    argsPaste(3).Name = "Transpose"
    argsPaste(3).Value = False
    argsPaste(4).Name = "AsLink"
    argsPaste(4).Value = False
    argsPaste(5).Name = "MoveMode"
    argsPaste(5).Value = 4
    dispatcher.executeDispatch(oController.Frame, ".uno:InsertContents", "", 0, argsPaste())

    MsgBox "Formats copied from 'format' to 'report_milk'."
End Sub