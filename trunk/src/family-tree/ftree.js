function init_ftree() {

  var people = {
    "id": "node00",
    "name": "Please select a person",
    "data": {},
    "children": [{
    	"id": "node13",  
        "name": "1.3",  
        "data": {},  
        "children": {}
    }]
};

      //Create a new canvas instance.
      var canvas = new Canvas('ftree_canvas', {
         //Where to inject canvas. Any HTML container will do.
         'injectInto':'family_tree',
         //Set width and height, default's to 200.
         'width': 900,
         'height': 500,
         //Set a background color in case the browser
         //does not support clearing a specific area.
        'backgroundColor': '#222'
      });
    //Create a new ST instance
    var st= new ST(canvas, {
      orientation : 'top',
      //set node and edge colors
      Node: {
       overridable: true,
       color: '#ccb'
      },
      Edge: {
        overridable: true,
        color: '#ccb'
      },
    //Add an event handler to the node when creating it.  
        onCreateLabel: function(label, node) {  
            label.id = node.id;  
            label.innerHTML = node.name;  
            label.onclick = function(){  
                st.onClick(node.id);  
            };  
        },  
        //This method is called right before plotting  
        //a node. It's useful for changing an individual node  
        //style properties before plotting it.  
        //The data properties prefixed with a dollar  
        //sign will override the global node style properties.  
        onBeforePlotNode: function(node) {  
            //add some color to the nodes in the path between the  
            //root node and the selected node.  
            if (node.selected) {  
                node.data.$color = "#ff7";  
            } else {  
                delete node.data.$color;  
            }  
        },  
  
        //This method is called right before plotting  
        //an edge. It's useful for changing an individual edge  
        //style properties before plotting it.  
        //Edge data properties prefixed with a dollar sign will  
        //override the Edge global style properties.  
        onBeforePlotLine: function(adj){  
            if (adj.nodeFrom.selected && adj.nodeTo.selected) {  
                adj.data.$color = "#eed";  
                adj.data.$lineWidth = 3;  
            }  
            else {  
                delete adj.data.$color;  
                delete adj.data.$lineWidth;  
            }  
        }  
    });

    
    //load json data
    st.loadJSON(people);
    //compute node positions and layout
    st.compute();
    //optional: make a translation of the tree
    //    ST.Geom.translate(st.tree,
    //      new Complex(-200, 0), "startPos");
    //Emulate a click on the root node.
    st.onClick(st.root);
   
    
    //Add input handler to switch spacetree orientation.  
      document.getElementById('orientation').onchange = function() {
       var select = document.getElementById('orientation');  
       var index = select.selectedIndex;  
       var or = select.options[index].value;  
       select.disabled = true;  
       st.switchPosition(or, "animate", {  
          onComplete: function() {  
            select.disabled = false;  
          }  
        });  
      }; 
      
      
      //Add input handler to switch spacetree orientation.  
      var select = document.getElementById('fbid');  
      select.onchange = function() { 
    	  
    	  var people = {
    			    "id": "node00",
    			    "name": select.value,
    			    "data": {},
    			    "children": [{
    			    	"id": "node96",  
    			        "name": "new" + select.value,  
    			        "data": {},  
    			        "children": {}
    			    }]
    			};
    	  people.name = select.value;
    	  st.op.morph(people, {});
    	  st.refresh();
      }; 
}


