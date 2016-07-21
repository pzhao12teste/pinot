<section id="self-service-existing-anomaly-functions">
    <script id="self-service-existing-anomaly-functions-template" type="text/x-handlebars-template">
        <table id="existing-anomaly-functions-table" class="display" >
            <thead>
            <tr>
                <th>Name</th>
                <th>Metric</th>
                <th>Properties</th>
                <th>Active</th>
                <th></th>
                <th></th>
            </tr>
            </thead>
            <tbody id="existing-anomaly-functions-tbody">
            {{#each this as |anomalyFunction anomalyFunctionIndex|}}
            <tr>
                <td>{{anomalyFunction/functionName}}</td>
                <td>{{anomalyFunction/metric}}</td>
                <td>{{anomalyFunction/properties}}</td>
                <td><input type="checkbox" {{#if anomalyFunction/isActive}}checked{{/if}} data-uk-modal="{target:'#toggle-alert-modal'}"></span></td>
                <td><span class="update-function-btn uk-button" data-row-id="{{anomalyFunctionIndex}}" data-uk-modal="{target:'#update-function-modal'}" data-uk-tooltip title="Edit"><i class="uk-icon-pencil"></i></span></td>
                <td><span class="uk-button" data-function-id="{{anomalyFunction/id}}" data-function-name="{{anomalyFunction/functionName}}" data-uk-modal="{target:'#delete-function-modal'}" data-uk-tooltip title="Delete"><i class="uk-icon-times"></i></span></td>
            </tr>
            {{/each}}
            </tbody>
        </table>
    </script>
</section>