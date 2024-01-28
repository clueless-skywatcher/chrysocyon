package com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params;

import lombok.Getter;

public class ViewCatalogParameters extends AbstractMetaTableParameters {
    private @Getter String name;
    private @Getter String definition;

    public ViewCatalogParameters(String viewName, String viewDef) {
        this.name = viewName;
        this.definition = viewDef;
    }
}
