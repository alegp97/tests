public final class WorkbookStyleManager {

    private static final String FONT = "Arial";

    private WorkbookStyleManager() {
        throw new IllegalAccessError("Non-instantiable class.");
    }

    public static XSSFCellStyle getTitleStyle(XSSFWorkbook wb) {
        XSSFCellStyle styleTitle = wb.createCellStyle();
        XSSFFont font = wb.createFont();
        font.setFontName(FONT);
        font.setBold(true);
        font.setFontHeightInPoints((short) 10);
        font.setColor(HSSFColor.BLACK.index);

        styleTitle.setFont(font);
        styleTitle.setAlignment(HorizontalAlignment.CENTER);
        return styleTitle;
    }

    public static XSSFCellStyle getHeaderStyle(XSSFWorkbook wb) {
        XSSFCellStyle styleHeader = wb.createCellStyle();
        XSSFFont font = wb.createFont();
        font.setFontName(FONT);
        font.setBold(true);
        font.setFontHeightInPoints((short) 10);
        font.setColor(HSSFColor.WHITE.index);

        styleHeader.setFont(font);
        BorderStyle thin = BorderStyle.THIN;
        short black = IndexedColors.BLACK.getIndex();
        styleHeader.setBorderRight(thin);  styleHeader.setRightBorderColor(black);
        styleHeader.setBorderBottom(thin); styleHeader.setBottomBorderColor(black);
        styleHeader.setBorderLeft(thin);   styleHeader.setLeftBorderColor(black);
        styleHeader.setBorderTop(thin);    styleHeader.setTopBorderColor(black);
        styleHeader.setAlignment(HorizontalAlignment.CENTER);
        styleHeader.setFillForegroundColor(IndexedColors.RED.getIndex());
        styleHeader.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        return styleHeader;
    }

    public static XSSFCellStyle getRegularStyle(XSSFWorkbook wb) {
        XSSFCellStyle styleRegular = wb.createCellStyle();
        XSSFFont font = wb.createFont();
        font.setFontName(FONT);
        font.setFontHeightInPoints((short) 10);
        font.setColor(HSSFColor.BLACK.index);

        styleRegular.setFont(font);
        BorderStyle thin = BorderStyle.THIN;
        short black = IndexedColors.BLACK.getIndex();
        styleRegular.setBorderRight(thin);  styleRegular.setRightBorderColor(black);
        styleRegular.setBorderBottom(thin); styleRegular.setBottomBorderColor(black);
        styleRegular.setBorderLeft(thin);   styleRegular.setLeftBorderColor(black);
        styleRegular.setBorderTop(thin);    styleRegular.setTopBorderColor(black);
        styleRegular.setAlignment(HorizontalAlignment.CENTER);
        return styleRegular;
    }
}
