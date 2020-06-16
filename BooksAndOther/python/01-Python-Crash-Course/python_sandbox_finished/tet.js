importClass(Packages.net.ifao.expense.validation.ValidationError);
importClass(Packages.net.ifao.expense.domain.links.StringLink);
importClass(Packages.net.ifao.expense.domain.links.LinkRoles);

var missingLinkDeclaration = element.getLink("expense.missingReceiptDeclaration");
if (missingLinkDeclaration && missingLinkDeclaration.getValue().equals("true")) {
log.fine("Missing receipt checked");
     var missingReceiptClarification = element.getLink("expense.missing.receipt.clarification");
      if(!missingReceiptClarification) {
        log.fine("Missing receipt clarification not provided");
        var vError = new ValidationError("error.expense.validation.statusenter");
        vError.label = configuration.getTranslatedText("error.expense.validation.statusenter", expense.getReportLocale());
        actionMarker.setActionSuccessfull(false);

    }
     actor.static.registerErrorWithElement(vError, element);
     actor.static.registerErrorWithElement(vError, expense);
}